import logging.config
import os
import sys
from os.path import join, dirname, expanduser

from pyhocon import ConfigFactory
from pyhocon import ConfigParser
from pyhocon import ConfigTree
from pyhocon import HOCONConverter


def get_parents(leaf, include_leaf=False):
    """
    List all parent directories of a file, e.g. for /home/user/python/my_project:
    - /home/user/python/my_project/ (only included if include_leaf=True)
    - /home/user/python/
    - /home/user/
    - /home/
    - /
    """
    if include_leaf:
        yield leaf
    dir = leaf
    seen = {leaf}
    while True:
        new_dir = dirname(dir)
        if new_dir == dir or new_dir in seen:
            break
        else:
            seen.add(new_dir)
            dir = new_dir
            yield dir


def find_files(name, cwd):
    """
    Try to find a file named `name` in locations relative to `cwd` and the users home directory.
    For example, for name="config.py" and cwd="/home/user/python/my_project", the following locations are returned:
    - /home/user/python/my_project/instance/config.py (usually used by applications like flask)
    - /home/user/python/my_project/config.py
    - /home/user/python/config.py
    - /home/user/config.py
    - /home/config.py
    - /config.py
    - ~/config.py (duplicate of /home/user/config.py!)
    - ~/.config.py
    """
    dirs = get_parents(join(cwd, "instance"), include_leaf=True)
    return [join(dir, name) for dir in dirs] + [  # ./instance/name, ./name, ../name, ../../name, ...
        join(expanduser('~'), name),  # ~/name
        join(expanduser('~'), "." + name)  # ~/.name
    ]


def load_config(cwd=os.getcwd(), debug=False):
    """
    Tries to find HOCON files named "iss4e.conf" using the paths returned by find_files().
    The found files are then parsed and merged together, so that a single configuration dict is returned.
    For details on HOCON syntax, see https://github.com/chimpler/pyhocon and https://github.com/typesafehub/config/

    Example configuration:
    - default config in home dir (~/iss4e.conf):
        datasources {
            influx {
                host = ${HOSTNAME}
                # also set your passwords (e.g. from env with ${MYSQL_PASSWD} here
            }
            mysql {
                host = localhost
            }
        }

    - local config in cwd (./iss4e.conf):
        webike {
            # use the generic information from ${datasources.influx} (should be defined in ~/iss4e.conf and contain
            # host, password, ...) and extend it to use the (non-generic) database "webike"
            influx = ${datasources.influx} {
                db = "webike"
            }
        }

    - merged config that will be returned:
        {
            "datasources": {
                "influx": {
                    "host": "SD959-LT"
                },
                "mysql": {
                    "host": "localhost"
                }
            },
            "webike": {
                "influx": {
                    "host": "SD959-LT", # copied from ~/iss4e.conf: datasources.influx
                    "db": "webike"
                }
            }
        }
    """
    # find "iss4e.conf" file in current working dir or parent directories
    files = find_files("iss4e.conf", cwd)
    configs = [ConfigFactory.parse_file(file, required=False, resolve=False) for file in files
               if os.path.isfile(file)]
    if debug:
        print("Config files:\n" + "\n".join(
            file + " [" + ("not " if not os.path.isfile(file) else "") + "found]" for file in files))
    # merge all levels of config
    config = ConfigTree(root=True)
    config.put("__main__", os.path.basename(sys.modules['__main__'].__file__))
    for c in configs:
        config = ConfigTree.merge_configs(c, config)
    config = ConfigParser.resolve_substitutions(config)
    if debug:
        print("Loaded config:\n" + HOCONConverter.to_json(config))

    # if config contains a key "logging", use it to reconfigure python logging
    if "logging" in config:
        if debug:
            print("Reconfiguring logging from config")
        if config.get("capture_exceptions", True):
            sys.excepthook = log_uncaught_exception
        logging.captureWarnings(config.get("capture_warnings", True))
        logging.config.dictConfig(config["logging"].as_plain_ordered_dict())
    return config


def log_uncaught_exception(type, value, tb):
    logging.exception("Uncaught exception: {0}".format(str(value)), exc_info=(type, value, tb))
    sys.__excepthook__(type, value, tb)
