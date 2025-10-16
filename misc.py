import os

def get_log_file(folder="Logs"):
    count = 1
    while os.path.isfile(f"{folder}/log_{count}"):
        count += 1
    with open(f"{folder}/log_{count}", 'x') as f:
        f.write("")
    return f"{folder}/log_{count}"