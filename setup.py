from setuptools import setup, find_packages

setup(
    name="followthemoney-graph",
    version="0.0.1",
    long_description="Followthemoney graph toolkit",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    keywords="",
    author="OCCRP",
    author_email="data@occrp.org",
    url="https://occrp.org",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "test"]),
    namespace_packages=[],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "followthemoney",
        "alephclient",
        "requests",
        "redis",
        "networkx[all]",
    ],
    entry_points={
        "followthemoney.graph": [
            "aleph = followthemoney_enrich.aleph:AlephGraph",
        ],
        # "followthemoney.cli": [
        # "graph = followthemoney_enrich.cli:graph",
        # ],
    },
    test_suite="nose.collector",
    tests_require=["coverage", "nose"],
)