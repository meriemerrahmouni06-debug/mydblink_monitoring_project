SELECT IndicatorName, Retention
FROM [msdb].[DBMonitor].[TBIndicatorCollecte]
ORDER BY IndicatorName;

SELECT * FROM sys.tables WHERE name = 'TBIndicatorCollecte';
#la table de config ou la retention est stockee 

SELECT COUNT(*) FROM [msdb].[DBMonitor].[TBIndicatorCollecte];
#affiche 0 la table est vide 
