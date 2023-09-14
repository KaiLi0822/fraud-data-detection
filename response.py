import sys
import requests
import json

def parse():
    taskid = str(sys.argv[1])
    info = str(sys.argv[2])
    message = str(sys.argv[3])
    pmmlfile = str(sys.argv[4])
    return taskid, info, message, pmmlfile


def return_post(taskid, info, message, pmmlfile):
    url = '***'

    if info == "SUCCESS":
        s = {
            "id": taskid,
            "status": info,
            "pmmlUrl": pmmlfile
        }
    else:
        s = {
            "id": taskid,
            "status": info,
            "failReason": message,
            "pmmlUrl": pmmlfile

        }

    headers = {'content-type': "application/json"}
    print(s)
    res = requests.post(url,headers=headers,data=json.dumps(s))
    print("服务端返回:"+res.text)

if __name__ == "__main__":
    taskid, info, message, pmmlfile = parse()
    return_post(taskid, info, message, pmmlfile)




