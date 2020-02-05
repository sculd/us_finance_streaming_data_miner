from setuptools import setup, find_packages

setup(
   name='us_finance_streaming_data_miner',
   version='0.35',
   description='A module to stream read us data',
   author='Hyungjun Lim',
   author_email='sculd3@gmail.com',
   packages=find_packages(include=['us_finance_streaming_data_miner', 'us_finance_streaming_data_miner.ingest', 'us_finance_streaming_data_miner.ingest.streaming']),
   install_requires=['pandas'],
)
