#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2018 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup


setup(name='medusa',
      version='0.0.1',
      author='Spotify',
      author_email='data-bye@spotify.com',
      description='Prototype',
      license='Apache',
      classifiers=[
          'Development Status :: 1 - Planning',
          'Environment :: Console',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3.6'
      ],
      python_requires='>=3',
      packages=('medusa',),
      entry_points={
          'console_scripts': [
              'medusa=medusa.medusacli:main',
          ]},
      scripts=['bin/medusa-wrapper']
      )
