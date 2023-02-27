import csv

def write_file(file, *args):
    writer = csv.writer(file)
    writer.writerow(args)