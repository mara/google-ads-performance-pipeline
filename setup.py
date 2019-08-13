from setuptools import setup, find_packages

setup(
    name='google-ads-performance-pipeline',
    version='1.0.0',
    description="A data integration pipeline that imports downloaded Google Ads performance data into a data warehouse",

    install_requires=[
        'google-ads-performance-downloader>=2.0.0',
        'mara-etl-tools>=1.1.0',
        'data-integration>=1.3.0'
    ],

    dependency_links=[
        'git+https://github.com/mara/google-ads-performance-downloader.git@2.0.0#egg=google-ads-performance-downloader-2.0.0',
        'git+https://github.com/mara/mara-etl-tools.git@1.1.0#egg=mara-etl-tools-1.1.0',
        'git+https://github.com/mara/data-integration.git@1.3.0#egg=data-integration-1.3.0'

    ],

    packages=find_packages(),

    author='Mara contributors',
    license='MIT'
)
