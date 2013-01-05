from setuptools import setup, find_packages
setup(
    name = "BucketBrigade",
    version = "0.1",
    packages = find_packages(),
    scripts = ['b2b.py'],
    install_requires = ['boto>=1.9']
)
