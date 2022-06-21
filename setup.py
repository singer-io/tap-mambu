#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-mambu',
      version='3.0.1',
      description='Singer.io tap for extracting data from the Mambu 2.0 API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mambu'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.23.0',
          'singer-python==5.12.1'
      ],
      extras_require={
          'dev': [
              'ipdb==0.11',
              'pylint==2.5.3',
              'pytest==6.2.4'
          ],
          'mambu-tests': [
              'coverage==6.3.1',
              'pylint==2.12.2',
              'pytest==6.2.5',
              'mock==4.0.3'
          ],
          'mambu-performance': [
              'matplotlib==3.5.1'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-mambu=tap_mambu:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_mambu': [
              'helpers/schemas/*.json'
          ]
      })
