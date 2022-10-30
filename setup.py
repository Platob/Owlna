import unittest

from setuptools import setup


def testsuite():
    return unittest.TestLoader().discover('tests', pattern='test_*.py')


setup(
    name='owlna',
    version='0.0.1',
    packages=['owlna'],
    url='https://github.com/Platob/Owlna.git',
    license='Apache',
    author='Platob',
    python_requires=">= 3.9",
    author_email='nfillot.pro@gmail.com',
    install_requires=open('requirements.txt').read().splitlines(),
    description='Athena with PyArrow',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Text Processing :: Linguistic',
    ],
    test_suite='setup.testsuite'
)
