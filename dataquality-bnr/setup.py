import setuptools
import os
import codecs

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

with open("requirements-dev.txt", "r") as fh:
    requirements_dev = fh.read()


with open("README.md", "r") as fh:
    long_description = fh.read()

here = os.path.abspath(os.path.dirname(__file__))

# Taken from https://github.com/pypa/pip/blob/master/setup.py#L19
def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    # intentionally *not* adding an encoding option to open, See:
    #   https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()

def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    raise RuntimeError("Unable to find version string.")


def get_name(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__name__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    raise RuntimeError("Unable to find name string.")

setuptools.setup(
    name=get_name("__init__.py"),
    version=get_version("__init__.py"),
    author="tz",
    author_email="",
    description="Pacote desenvolvido como extensao deequ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.santanderbr.corp/tz/PYTHON/dataquality-bnr",
    packages=setuptools.find_packages(),
    package_data = {
        'dataquality_bnr': ['static/deequ-1.0.5.jar']
    },
    install_requires=requirements,
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: POSIX :: Linux",
    ],
)
