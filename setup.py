from setuptools import setup, find_packages

setup(
    name="cnstlltn",
    setup_requires=["setuptools_scm"],
    use_scm_version=True,
    python_requires=">=3.6, <=3.7",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={"console_scripts": ["cnstlltn = cnstlltn._cli:main"]},
    install_requires=[
        'ansimarkup>=1.4,<2',
        'atomicwrites>=1.2.1,<2',
        'attrdict>=2,<3',
        'braceexpand>=0.1.2,<1',
        'click>=7.0,<8',
        'filelock>=3.0.8,<4',
        'formic2>=1.0.3,<2',
        'graphviz>=0.10.1,<1',
        'jinja2>=2.10,<3',
        'toposort>=1.5,<2',
        'zope.interface>=4.5,<5',
    ],
    extras_require={
        'aws': ['boto3~=1.9.5']
    }
)
