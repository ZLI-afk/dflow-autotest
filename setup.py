import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dflow-props",
    version="0.0.1",
    author="Zhuoyuan Li",
    author_email="zhuoyli@outlook.com",
    description="A properties test package based on dflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),
    install_requires=[
        "pydflow>=1.6.27",
        "lbg>=1.2.13",
        "dpdata>=0.2.13",
        "matplotlib",
        "dpgen",
        "seekpath"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    provides=["dflowprops"],
    script=["scripts/__main__"]
)
