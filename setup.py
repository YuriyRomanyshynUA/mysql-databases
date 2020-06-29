import setuptools


setuptools.setup(
    name="mysql-connector-wrapper-pkg-yuriy-romanyshyn",
    version="0.0.1",
    author="Yuriy Romanyshyn",
    author_email="yuriy.romanyshyn.ua.lv@gmail.com",
    description="mysql-connector driver wrapper",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=["mysql-connector-python"],
    python_requires='>=3.6'
)
