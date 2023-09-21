from setuptools import setup, find_packages

setup(
    name='mbpy_plugin_cumulative_attendance',
    version='0.3',
    packages=find_packages(),
    entry_points='''
        [mbpy_plugins]
        cumulative-hr-attendance = mbpy_plugin_cumulative_attendance.homerooms:cli
        cumulative-class-attendance = mbpy_plugin_cumulative_attendance.classes:cli
        cumulative-combo-attendance = mbpy_plugin_cumulative_attendance.combo:cli
    ''',
)
