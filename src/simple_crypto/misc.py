import os
import pandas
import aiofiles

def get_log_file(folder="Logs"):
    count = 1
    while os.path.isfile(f"{folder}/log_{count}"):
        count += 1
    with open(f"{folder}/log_{count}", 'x') as f:
        f.write("")
    return f"{folder}/log_{count}"

async def async_csv_to_df(file_path: str):
    async with aiofiles.open(file_path, mode='r') as f:
        content = await f.read()
    from io import StringIO
    return pandas.read_csv(StringIO(content))