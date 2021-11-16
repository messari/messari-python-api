from setuptools import setup

# read the contents of your README file
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


setup(
    name='messari',
    version='0.0.1',
    packages=['tests', 'messari', 'messari.defillama', 'messari.messari', 'messari.tokenterminal'],
    url='',
    long_description=long_description,
    long_description_content_type='text/markdown',
    data_files=['json/messari_to_dl.json', 'json/messari_to_tt.json'],
    license='MIT`',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    author='Roberto Talamas, Michael Kremer',
    author_email='roberto.talamas@gmail.com, kremeremichael@gmail.com',
    description='Messari API'
)
