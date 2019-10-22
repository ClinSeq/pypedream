from setuptools import setup
from pip.req import parse_requirements

# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements("requirements.txt", session=False)

# reqs is a list of requirement
reqs = [str(ir.req) for ir in install_reqs if ir.req is not None]

setup(name='pypedream_test',
      version='0.6.5',
      packages=['pypedream_test', 'pypedream_test.pipeline', 'pypedream_test.runners', 'pypedream_test.tools'],
      install_requires=reqs
      )
