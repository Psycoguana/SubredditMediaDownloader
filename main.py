import asyncio
import datetime
import json
import logging
import pathlib
import re
import time
import warnings
from configparser import ConfigParser

import aiofiles
import aiohttp
import ffmpeg
from psaw import PushshiftAPI
from tqdm import tqdm, trange
from tqdm.asyncio import tqdm as async_tqdm

from utils import retry_connection, get_logger


class SubredditDownloader:
    def __init__(self):
        self.log = get_logger(__name__, logging.DEBUG)
        self.config = ConfigParser()
        self.config.read('config.ini')
        self.bot_config = self.config['BOT']

        # Turn off warnings.
        warnings.filterwarnings('ignore')

        self.api = PushshiftAPI()
        self.session = self.set_session()

    @staticmethod
    def set_session():
        # Some image hosts (ahyes.fun) were throwing ssl errors, so we stop verifying certificates.
        conn = aiohttp.TCPConnector(limit=10, ssl=False)

        # If the subreddit is big, it could take a long time to download everything
        # and sooner or later the session expires, so we'll just disable timeouts.
        timeout = aiohttp.ClientTimeout(total=None)

        return aiohttp.ClientSession(connector=conn, timeout=timeout)

    async def run(self):
        # Get the total amount of submissions to download, this allows us to set the progressbar.
        try:
            total_submissions = await self.get_submissions_amount()
        except RuntimeError:
            print("No images found. Quitting...")
            return

        submissions = await self.get_submissions()
        print("\nSearching posts...", flush=True)
        elements = await self.get_elements_info(submissions, total_submissions)

        print("\nDownloading posts...", flush=True)
        await self.download_elements(elements)

    async def download_elements(self, links: dict):
        pattern = r'\.(jpe?g|gif?v|png|mp4)'
        tasks = []
        for name, link in links.items():
            match = re.search(pattern, link)
            # Add the proper extension (png|jpg|mp4|gif) to the name.
            try:
                name += '.' + match.group(1)
            except AttributeError:
                if 'v.redd.it' in link:
                    # Sometimes links don't have a file extension.
                    # See here: https://v.redd.it/gyh95hiqc0b11/DASH_9_6_M?source=fallback
                    name += '.mp4'
                else:
                    print(f"Unrecognized link skipped. {link}")
                    continue

            tasks.append(asyncio.create_task(self.download(name=name, url=link)))

        await async_tqdm.gather(*tasks, colour='green')

    async def get_submissions_amount(self):
        """ Get the total number of submissions """
        submissions = await self.get_submissions(ask_len=True)
        next(submissions)
        return self.api.metadata_['es']['hits']['total']['value']

    async def get_submissions(self, ask_len=False):
        subreddit = self.bot_config['SUBREDDIT']
        # If we only want to know the total amount of submissions,
        # we can set a limit of 1 to be kind to PushShift api.
        limit = 1 if ask_len else None

        date_config = self.config['DATES']
        before = date_config['BEFORE'] or ''
        after = date_config['AFTER'] or ''

        if ask_len:
            subreddit = self.bot_config['SUBREDDIT']
            if after and before:
                print(f"Scraping images from r/{subreddit} before {before} and after {after}")
            elif before:
                print(f"Scraping images from r/{subreddit} before {before}")
            elif after:
                print(f"Scraping images from r/{subreddit} after {after}")
            else:
                print(f"Scraping all images from r/{subreddit} ")

        try:
            if before:
                before = int(datetime.datetime.strptime(before, '%Y-%m-%d').timestamp())
            if after:
                after = int(datetime.datetime.strptime(after, '%Y-%m-%d').timestamp())
        except ValueError:
            print("Date format is wrong. Please use YYYY-MM-DD")
            print("Quitting...")
            await self.session.close()
            exit()

        return self.api.search_submissions(limit=limit,
                                           subreddit=subreddit,
                                           before=before,
                                           after=after,
                                           fields=['id',
                                                   'crosspost_parent_list',
                                                   'media',
                                                   'media_metadata',
                                                   'url',
                                                   'permalink']
                                           )

    async def get_elements_info(self, submissions, submissions_len) -> dict:
        elements = {}

        with tqdm(total=submissions_len, colour='green') as pbar:
            for sub in submissions:
                if not hasattr(sub, 'url'):
                    # Update progress bar status
                    pbar.update(1)
                    continue
                if re.search(r'\.(jpg|gif|png)$', sub.url):
                    elements[sub.id] = sub.url
                elif re.search(r'\.gifv$', sub.url):
                    link = await self.get_real_gif_link(sub.url)
                    if link:
                        elements[sub.id] = link
                elif sub.url.startswith('https://www.reddit.com/gallery/'):
                    try:
                        images = await self.parse_image(sub.id, sub.media_metadata)
                        for key, value in images.items():
                            elements[key] = value
                    except AttributeError:
                        # This happens with removed posts.
                        pass

                elif sub.url.startswith('https://v.redd.it/'):
                    video = await self.parse_video(sub)
                    if video:
                        elements[sub.id] = video
                else:
                    # External link. Ignore it.
                    pass
                # Update progress bar status
                pbar.update(1)
        return elements

    async def get_real_gif_link(self, link):
        # Imgur does a very strange thing where their .gifv are actually just .mp4,
        # so we need the real link to the video.
        async with self.session.get(link) as resp:
            data = await resp.read()
            # Convert bytes to str.
            try:
                data = data.decode('utf-8')
                match = re.findall(r'content="(.+mp4)', data)
            except UnicodeDecodeError:
                print(f"Wrong encoding format for {link}. Skipped.")
                return ''

        return '' if not match else match[0]

    @retry_connection
    async def download(self, name, url) -> None:
        async with self.session.get(url) as response:
            if response.status == 404 or response.status == 403:
                # Image/Video has been deleted.
                # It's not a mistake, Reddit responds with 403 statuses with their deleted hosted videos.
                # See here: https://www.reddit.com/q1567e
                # And here: https://v.redd.it/stx7a2b1ofr71/DASH_720.mp4?source=fallback
                return

            content = await response.read()

            if url.startswith('https://v.redd.it'):
                await self.download_reddit_video(name, url, video_data=content)
            else:
                await self.write_to_disk(name=name, image=content)

    async def download_reddit_video(self, name, url, video_data):
        # Download video's audio.
        audio_link = re.sub(r'DASH_(\d{3,4})', 'DASH_audio', url)
        async with self.session.get(audio_link) as audio_response:
            audio_data = await audio_response.read()

        # Store them into files.
        temp_video_file = f'{name}_temp.mp4'
        temp_audio_file = f'{name}_audio_temp.mp4'
        async with aiofiles.open(temp_video_file, 'wb') as file:
            await file.write(video_data)

        async with aiofiles.open(temp_audio_file, 'wb') as file:
            await file.write(audio_data)

        # Load them into ffmpeg and join them.
        input_video = ffmpeg.input(temp_video_file)
        input_audio = ffmpeg.input(temp_audio_file)

        dir_path = await self.get_file_dst_folder(name)
        dst_file = str(dir_path / name)

        stream = ffmpeg.output(input_video,
                               input_audio,
                               filename=dst_file,
                               vcodec='copy',
                               acodec='copy',
                               loglevel='quiet')
        try:
            stream.run(overwrite_output=True)
        except ffmpeg.Error:
            # Video probably has no audio.
            await self.write_to_disk(name=name, image=video_data)

        # Delete temporary files.
        pathlib.Path(temp_video_file).unlink()
        pathlib.Path(temp_audio_file).unlink()

    async def write_to_disk(self, name, image):
        """ Write the downloaded image/video/gif into the corresponding folder """
        dir_path = await self.get_file_dst_folder(name)

        file_path = dir_path / name
        f = await aiofiles.open(file_path, mode='wb')
        await f.write(image)
        await f.close()

    async def get_file_dst_folder(self, name):
        if name.endswith('mp4'):
            sub_folder = 'videos'
        elif name.endswith('gif') or name.endswith('gifv'):
            sub_folder = 'gifs'
        else:
            sub_folder = 'images'

        dir_path = pathlib.Path(self.bot_config['DOWNLOAD_FOLDER']) / self.bot_config['SUBREDDIT'] / sub_folder
        try:
            dir_path.mkdir(parents=True, exist_ok=True)
            return dir_path
        except FileNotFoundError as error:
            print(error)
            print("Is your Download folder written correctly?")
            await self.session.close()
            exit()

    @staticmethod
    async def parse_image(id_, images):
        images_dict = {}

        for img_num, image in enumerate(images.values(), start=1):
            if image['status'] != 'completed':
                # Image was not processed? Does not contain any more info.
                continue

            url = image['s']['u']
            # Fix for api changes in images url. See here: https://reddit.com/9ncg2r
            url = url.replace('amp;', '')
            images_dict[id_ + f'_{img_num}'] = url

        return images_dict

    @retry_connection
    async def parse_video(self, submission):
        try:
            video = submission.crosspost_parent_list[0]['media']['reddit_video']
            if video['transcoding_status'] != 'completed':
                # Video didn't transcode correctly or was deleted?
                return

            return video['fallback_url']
        except TypeError:
            # Image was deleted.
            return
        except AttributeError:
            return await self.download_video_with_json(submission)

        except Exception:
            raise

    async def download_video_with_json(self, submission) -> str:
        # The submission has not been crossposted, so to get the video information
        # we need to open the v.redd.it link, replace the end with a .json and get the video link from there.
        headers = {
            'authority': 'www.reddit.com',
            'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'dnt': '1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-user': '?1',
            'sec-fetch-dest': 'document',
            'accept-language': 'en,es-ES;q=0.9,es;q=0.8',
        }

        link = f'https://www.reddit.com{submission.permalink}.json'
        async with self.session.get(link, headers=headers) as response:
            if response.status == 429:
                print("Too many requests. Sleeping 5 minutes and trying again...")
                for _ in trange(5 * 60):
                    time.sleep(1)
                return await self.download_video_with_json(submission)

            try:
                response = await response.json()
            except json.decoder.JSONDecodeError as error:
                print("Error downloading video...")
                print(f"{type(error).__name__}: {error}")
                return ''

            try:
                media = response[0]['data']['children'][0]['data']['secure_media']
                if not media:
                    # Video was probably removed before the video was transcoded.
                    return ''

                video = media['reddit_video']
                if video['transcoding_status'] != 'completed':
                    # Video didn't transcode correctly?
                    return ''

                return video['fallback_url']
            except TypeError as error:
                print("Error downloading video...")
                print(f"{type(error).__name__}: {error}")


async def main():
    t0 = time.perf_counter()

    downloader = SubredditDownloader()
    try:
        await downloader.run()
    except KeyboardInterrupt:
        print("Downloads cancelled. Goodbye!")
    except Exception:
        raise
    finally:
        if downloader.session:
            await downloader.session.close()

        print(f"\nExec time: {((time.perf_counter() - t0) / 60):.2f} minutes.")


if __name__ == '__main__':
    asyncio.run(main())
