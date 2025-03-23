# Al-Meezan-Fund-Tracker
Server side for parsing al meezan investments daily fund performance emails and keeping history

### Remove duplicates
```sql
DELETE FROM funds 
WHERE id NOT IN (
    SELECT MIN(id) 
    FROM funds 
    GROUP BY name, upload_date
);
```
