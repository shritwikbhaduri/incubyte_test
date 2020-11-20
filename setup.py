from setuptools import setup

setup(
    name='etl',
    version='0.1',
    py_modules=['etl'],
    install_requires=['click'],
    entry_points='''
        [console_scripts]
        etl=etl.main:main
    ''',
)
