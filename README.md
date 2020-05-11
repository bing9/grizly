# Grizly

Grizly is a highly experimental library to generate SQL statements using the python pandas api, generate workflows and more.

## Docs

https://grizly-docs.readthedocs.io/en/0.3/index.html


## Installation
There are two ways to install grizly
### Installation with pip
```
> git clone https://github.com/kfk/grizly.git
> cd grizly
> pip install -r requirements.txt
> pip install grizly
```

### Installation with setuptools
```
> git clone https://github.com/kfk/grizly.git
> cd grizly
> pip install -r requirements.txt
> python setup.py install
```

## How to contribute?

Any contribution is welcome. You can work on some existing issues or if you noticed any bug or want to add some enhancements - please create an issue first.

### Before you start your work
- choose an issue or create new one
- create a branch from currently developed branch - the name will be always the same as the version that we are working on, eg. `0.3.3`
- name of your branch should correspond to what you will be working on

### After finishing your work
- update `changelogs.md`
- merge your branch with currently developed branch
- remove your branch
- close the issue
