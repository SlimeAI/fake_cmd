from setuptools import setup, find_packages
from fake_cmd import __version__

README = 'README.md'

setup(
    name='fake_cmd',
    version=__version__,
    packages=find_packages(include=['fake_cmd']),
    include_package_data=False,
    entry_points={},
    install_requires=[],
    url='https://github.com/SlimeAI/fake_cmd',
    author='SlimeAI',
    author_email='liuzikang0625@gmail.com',
    license='MIT License',
    description=(
        'fake_cmd provides a simulated command line based on file communication to send commands '
        'from the client to the server for execution.'
    ),
    long_description=open(README, encoding='utf-8').read(),
    long_description_content_type='text/markdown'
)
