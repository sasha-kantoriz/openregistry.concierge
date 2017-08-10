from setuptools import setup, find_packages
import os

version = '1.0'

requires = [
    'pyyaml',
    'couchdb',
    'requests'
]

entry_points = {
    'console_scripts': [
        'labot_worker = openregistry.labot.worker.worker:main'
    ]
}

setup(name='openregistry.labot.worker',
      version=version,
      description="openregistry.labot.worker",
      long_description=open("README.txt").read() + "\n" +
                       open(os.path.join("docs", "HISTORY.txt")).read(),
      # Get more strings from
      # http://pypi.python.org/pypi?:action=list_classifiers
      classifiers=[
        "Programming Language :: Python",
        ],
      keywords='',
      author='',
      author_email='',
      url='http://svn.plone.org/svn/collective/',
      license='GPL',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['openregistry'],
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      entry_points=entry_points
      )
