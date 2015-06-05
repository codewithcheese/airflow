from setuptools import setup, find_packages

setup(
    # Application name:
    name="stat_daemon",
    # Version number (initial):
    version="0.1.0",
    # Application author details:
    author="Aaron Keys",
    author_email="aaron.keys@airbnb.com",
    # Packages
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    # Include additional files into the package
    #include_package_data=True,
    scripts=['stat_daemon/stat_daemon', ],
    # Details
    url="",
    # license="LICENSE.txt",
    description="A metadata collection process.",
    # long_description=open("README.txt").read(),
    # Dependent packages (distributions)
    install_requires=[
        #aiflow (requires dev version for now...)
    ],
)