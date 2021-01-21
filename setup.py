from setuptools import setup


with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='process_decorator',
    version='0.6',
    packages=['process_decorator', 'process_decorator.shared'],
    url='https://github.com/Saylermb/process_decorator',
    license='',
    author='saylermb',
    author_email='Saylermb@gmail.com',
    description='make func async and execute in other process',
    long_description=long_description,
    long_description_content_type='text/markdown',
    python_requires='>=3.7.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Hardware :: Symmetric Multi-processing',
        'Framework :: AsyncIO',
        'Topic :: Utilities']

)
