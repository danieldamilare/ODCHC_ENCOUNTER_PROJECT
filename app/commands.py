import click
import unittest
import sys
from app import app
@click.command('run-test')
def run_test_command():
    """
    Runs the unit tests for the application.
    Discovers all tests in the project and runs them.
    """
    tests = unittest.TestLoader().discover('.')
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(tests)
    if not result.wasSuccessful():
        sys.exit(1)
    click.echo('All tests passed!')
