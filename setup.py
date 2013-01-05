from setuptools import setup, find_packages
setup(
    name = "BucketBrigade",
    version = "0.1",
    packages = find_packages(),
    scripts = ['b2b.py'],
    install_requires = ['boto>=1.9'],
    package_data = {
            '':['*.job','*.md']
            },
    author="Ajish George",
    author_email="root@rootedinsights.com",
    description="A simple utility to allow syncing/copying S3 bucket files across AWS accounts.",
    url="http://github.com/uhjish/bucketbrigade"
)
