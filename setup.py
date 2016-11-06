from codecs import open
from os import path

from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='iss4e-toolchain',
    version='0.1.0',
    description='iss4e python toolchain',
    long_description=long_description,
    url='https://github.com/iss4e/iss4e-toolchain',
    author='Information Systems and Science for Energy',
    author_email='webike-dev@lists.uwaterloo.ca',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
    ],
    packages=find_packages(),
    install_requires=[
        # SciPy tool for scientific computing and plotting
        'scipy>=0.18.1',
        'numpy>=1.11.2',
        'matplotlib>=2.0.0b4',
        # Database drivers
        'influxdb>=3.0.0',
        'PyMySQL>=0.7.9',
        # date and time utils (*always use UTC*)
        'python-dateutil>=2.5.3',
        'pytz>=2016.7',
        # further tools
        'PyYAML>=3.12',
        'more-itertools>=2.2',
        'tabulate>=0.7.5',
        'pyhocon>=0.3.34'
    ]
)
