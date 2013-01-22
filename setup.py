#!/usr/bin/env python

from distutils.core import setup
import os
import sys

if sys.version_info < (2, 5):
    raise NotImplementedError("Sorry, you need at least Python 2.5 to use Binder.")
if sys.version_info >= (3, 0):
    raise NotImplementedError("Sorry, Binder does not support Python 3.")

setup(
    name='binder',
    description='Binder is a lightweight mapping engine for SQL databases.',
    version='0.6',
    url='https://github.com/divtxt',
    license='MIT',
    author='Div Shekhar',
    author_email='div@txtlabs.com',
    package_dir={'': 'src'},
    packages=['binder'],
    platforms='any',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        ],
    )
