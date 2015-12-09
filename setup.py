#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect
import os
import platform
import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

__location__ = os.path.join(
    os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe())))


def read_version(package):
    with open(os.path.join(package, '__init__.py'), 'r') as fd:
        for line in fd:
            if line.startswith('__version__ = '):
                return line.split()[-1].strip().strip("'")


version = read_version('queue_utils')

py_major_version, py_minor_version, _ = (
    int(v.rstrip('+')) for v in platform.python_version_tuple())


def get_install_requirements(path):
    content = open(os.path.join(__location__, path)).read()
    requires = [req for req in content.split('\\n') if req != '']
    return requires


class PyTest(TestCommand):

    user_options = [('cov-html=', None, 'Generate junit html report')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.cov = None
        self.pytest_args = ['--cov', 'queue_utils', '--cov-report',
                            'term-missing']
        self.cov_html = False

    def finalize_options(self):
        TestCommand.finalize_options(self)
        if self.cov_html:
            self.pytest_args.extend(['--cov-report', 'html'])

    def run_tests(self):
        import pytest

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


setup(name='queue_utils',
      packages=find_packages(),
      version=version,
      description='A set of utilities for tasks based on RabbitMQ',
      long_description=open('README.rst').read(),
      url='https://github.com/zalando/queue_utils',
      author='Zalando SE',
      license='MIT',
      setup_requires=['flake8'],
      install_requires=get_install_requirements('requirements.txt'),
      tests_require=['pytest-cov', 'pytest'],
      cmdclass={'test': PyTest},
      test_suite='tests',
      include_package_data=True,)
