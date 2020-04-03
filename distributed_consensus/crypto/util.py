
def read_all_str(file: str):
    with open(file, 'r') as f:
        return f.read(-1)
