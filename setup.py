from setuptools import setup

setup(
        name='segway.mdseg',
        version='1.0',
        url='https://github.com/htem/segway.mdseg',
        author='Tri Nguyen',
        author_email='tri_nguyen@hms.harvard.edu',
        license='MIT',
        packages=[
            'segway.mdseg',
            'segway.mdseg.database',
        ],
        install_requires=[
            "funlib.math",
            "funlib.geometry",
            "pymongo",
        ]
)
