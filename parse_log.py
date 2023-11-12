try:
    from parse import parse
    import os
    import csv
except:
    print("Haven't install the parse module ")
    print("Please use 'pip install parse' install parse module ")
    exit(0)


file_path = input("please input the log path")

if not os.path.isfile(file_path):
    print("The file_path not currently, please check again")
    exit(0)

f = open(file_path)


with open("parsed.csv", "w") as csvfile:
    writer = csv.writer(csvfile)
    logs = f.readlines()
    for single_log in logs:
        single_log = single_log.strip('\n')
        result = parse("[{}] {} {} [{}] {} ({}) [{}][{}][{}][{}] [{}] {}", single_log)
        if result:
            writer.writerow([ele for ele in result])
        else:
            continue
print("Finish parse")
