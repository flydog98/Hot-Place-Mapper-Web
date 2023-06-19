from flask import Flask, render_template
from flask import request
import time as t
import boto3
import botocore
import paramiko

app = Flask(__name__)

@app.route('/')
def home():
  return render_template('index.html')

@app.route('/result')
def result():
  # map-reduce 검색

  key = paramiko.RSAKey.from_private_key_file("./config/dsc.pem")
  client = paramiko.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  result = ""

  try:
    client.connect(hostname="ec2-3-34-205-23.ap-northeast-2.compute.amazonaws.com", username="hadoop", pkey=key)

    print("request map-reduce")
    time = request.args.get('time')
    category = request.args.get('category')
    stdin, stdout, stderr = client.exec_command(f"hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -mapper 'python3 mapper.py' -file /home/hadoop/mapper.py -reducer 'python3 reducer.py {time}' -file /home/hadoop/reducer.py -input /input/ -output /output")

    t.sleep(7)

    print("reading file")
    stdin, stdout, stderr = client.exec_command("hdfs dfs -cat /output/*")
    result = stdout.read().decode('utf-8')

    print("removing file")
    stdin, _, stderr = client.exec_command("hdfs dfs -rm -R /output")

    # stdin, _, stderr = client.exec_command("docker exec namenode1 /home/hadoop-3.3.0/bin/hdfs dfs -rm -R /output")

    client.close()

  except Exception as e:
    print(e)
  
  # stdout = b'place1,6 30\nplace2,1 123\nplace99,12  999\nplace44322,1 3'

  lines = list()

  for input in result.splitlines():
    # print("input:", input)
    comma_pos = input.find(",")
    tab_pos = input.find("\t")
    dummy = list()
    dummy.append(input[:comma_pos])
    dummy.append(input[comma_pos + 1:comma_pos + 3])
    dummy.append(input[tab_pos + 1:])
    # print("dummy:", dummy)
    lines.append(dummy)

  lines.sort(key = lambda x: -int(x[2]))

  # print(lines)
  
  return render_template('result.html', result = lines[:5])
  # return render_template('result.html')

if __name__ == '__main__':
  app.run(port="1234", debug=True)
