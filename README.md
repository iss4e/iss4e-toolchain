# ISS4E python toolchain

This repository contains common utilities used for various python projects at the Information Systems and Science for
Energy laboratory at University of Waterloo.

# How to use and contribute

The code in this repository follows the standard python coding conventions as defined in 
[PEP 8](https://www.python.org/dev/peps/pep-0008/).
The main points (plus our peculiarities) are:
- always use UTF-8 and UNIX-style "\n" line endings
- 4 spaces for indetation
- module, file, local variable and function names `lowercase_with_underscores`
- only class names CamelCase
- see the __init__.py in each directory for details like the exported symbols
- python version is 3.5

All dependencies and general information about this project is declared in setup.py.
Use `python3 setup.py develop` (if required either with prefix `sudo` or  suffix `--user`) to install on your local
system.

The file .gitignore contains rules that should help prevent you from commit any files containing passwords.
