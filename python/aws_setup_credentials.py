#!/usr/bin/env python3.7
import subprocess
import os
import argparse
import json

parser = argparse.ArgumentParser(description='MFA CODE')
parser.add_argument('--code', '-c', type=str, help='MFA CODE')

args = parser.parse_args();
cmd = 'aws sts get-session-token --serial-number arn:aws:iam::429750608758:mfa/hailong.shi --profile earnin --token-code ' + args.code + ' > tmp.json'
p = subprocess.Popen(cmd, shell=True)
p.wait()

with open('tmp.json') as json_file:
    data = json.load(json_file)

id = 'aws_access_key_id = ' + data['Credentials']['AccessKeyId'] + '\n'
key = 'aws_secret_access_key = ' + data['Credentials']['SecretAccessKey'] + '\n'
token = 'aws_session_token = ' + data['Credentials']['SessionToken'] + '\n'

with open('credentials','r') as input: 
    lines = input.readlines()

lines[1]=id
lines[2]=key
lines[3]=token
with open('credentials','w') as output: 
    output.writelines(lines)
with open('Z:/.aws/credentials','w') as output: 
    output.writelines(lines)
os.remove("tmp.json")