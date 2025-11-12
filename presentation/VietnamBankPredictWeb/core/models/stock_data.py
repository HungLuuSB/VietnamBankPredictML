from django.db import models

class StockData(models.Model):
    symbol = models.TextField(blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)
    open = models.FloatField(blank=True, null=True)
    low = models.FloatField(blank=True, null=True)
    close = models.FloatField(blank=True, null=True)
    volume = models.FloatField(blank=True, null=True)
    
    class Meta:
        managed = False


