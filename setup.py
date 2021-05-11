import os

def set_youtube_api():
    api_key = input('Enter Google API Key: ')
    if api_key is not None and len(api_key) > 0:
        os.environ['GOOGLE_API'] = api_key


def setup():
    set_youtube_api()


if __name__ == "__main__":
    setup()