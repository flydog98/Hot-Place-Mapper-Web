from flask import Flask, render_template
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

  try:
    client.connect(hostname="ec2-15-165-39-26.ap-northeast-2.compute.amazonaws.com", username="ubuntu", pkey=key)

    stdin, stdout, stderr = client.exec_command("docker exec namenode1 ls")

    print(stdout.read())

    client.close()

  except Exception as e:
    print(e)
  
  return render_template('result.html')

if __name__ == '__main__':
  app.run(debug=True)