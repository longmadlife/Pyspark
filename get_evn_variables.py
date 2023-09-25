import os

os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
header = os.environ['header']
inferSchema = os.environ['inferSchema']
envn = os.environ['envn']
appName = 'pyspark project'
current = os.getcwd()
src_olap = current + r'\source\olap'
src_oltp = current + r'\source\oltp'


