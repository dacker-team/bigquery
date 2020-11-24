from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

setup(
    name='bigquery',
    version='0.0.1',
    description='Easily send data to Big Query',
    long_description=readme,
    author='Dacker',
    author_email='hello@dacker.co',
    url='https://github.com/dacker-team/pyred',
    keywords='send data google big query easy',
    packages=find_packages(exclude=('tests', 'docs')),
    python_requires='>=3',
    install_requires=[
        "psycopg2-binary>=2.7.4",
        "pandas>=0.25.0",
        "dbstream>=0.0.19"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
