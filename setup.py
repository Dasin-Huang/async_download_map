from setuptools import setup

setup(
    name='async_download_map',
    version='1.0.0',
    packages=['async_download_map'],
    url='',
    license='',
    author='dasin',
    author_email='daisn_huang@qq.com',
    description='异步高并发爬取地图瓦片',
    install_requires=['requests>=2.31.0', 'httpx>=0.27.0', 'aiohttp>=3.9.2', 'tqdm>=4.66.1'],
    python_requires='>=3.11',
)
