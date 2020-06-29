import setuptools


setuptools.setup(
    name="mysql-databases-pkg-yuriy-romanyshyn",
    version="0.0.1",
    author="Yuriy Romanyshyn",
    author_email="yuriy.romanyshyn.ua.lv@gmail.com",
    description="mysql-databases connections manager",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=["mysql-connector-python"],
    python_requires='>=3.6'
)
