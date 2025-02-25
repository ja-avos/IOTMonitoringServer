from django.core.management.base import BaseCommand
from IOTMonitoringServer import utils


class Command(BaseCommand):
    help = 'Generates mock data for testing the database performance'

    def handle(self, *args, **kwargs):
        if len(args) == 0:
            data_qty = 20000
        else:
            data_qty = int(args[0])
        utils.register_users()
        utils.generateMockData(data_qty)
