# 🚀 Quick Start Guide - Silver Layer with Reference Data

## ✅ Deployment Status

**Infrastructure**: ✅ Deployed and Live  
**Testing**: ⚠️ Blocked (requires Bronze layer fix)

---

## 📋 What's Deployed

### Resources (19 total)
- ✅ IAM Role + 3 Policies
- ✅ 4 S3 Objects (scripts + ref data)
- ✅ 1 Glue Database (`brandrep_silver`)
- ✅ 4 Glue Tables
- ✅ 2 Glue Jobs

### Reference Data
- **File**: `smartphones_models.csv`
- **Location**: `s3://.../silver/ref_data/seeds/smartphones_models.csv`
- **Columns**: 23 (22 original + 1 derived)
- **Records**: 982 smartphone models

---

## 🔧 To Test (After Bronze Fix)

### 1. Run Silver-Core Job
```bash
aws glue start-job-run \
  --job-name tf-prod-smartcomp-glue-enrichment-reddit-silver-core \
  --arguments '{"--PROCESS_DATE":"10-12-2024"}' \
  --region us-east-1
```

### 2. Run Silver-Sentiment Job  
```bash
aws glue start-job-run \
  --job-name tf-prod-smartcomp-glue-enrichment-reddit-silver-sentiment \
  --arguments '{"--PROCESS_DATE":"10-12-2024"}' \
  --region us-east-1
```

### 3. Check Reference Data
```sql
SELECT COUNT(*) as total_models,
       COUNT(DISTINCT brand) as total_brands
FROM brandrep_silver.dim_products_ref;
-- Expected: 982 models, ~40 brands
```

---

## ⚠️ Known Issue

**Problem**: Bronze tables missing `TableType` metadata  
**Fix Required**: Update `aws/us-east-1/21_glue/main.tf`  
**Status**: Blocking end-to-end tests

### Fix Bronze Tables
Add to each Bronze table definition:
```hcl
table_type = "ICEBERG"

storage_descriptor {
  input_format  = "org.apache.hadoop.mapred.SequenceFileInputFormat"
  output_format = "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"
  # ... rest of config
}
```

---

## 📊 Quick Queries

### Check Silver Tables
```sql
-- Submissions
SELECT COUNT(*), 
       SUM(CASE WHEN is_usable_for_sentiment THEN 1 END) as usable
FROM brandrep_silver.submissions_silver;

-- Comments  
SELECT COUNT(*),
       SUM(CASE WHEN is_usable_for_sentiment THEN 1 END) as usable
FROM brandrep_silver.comments_silver;
```

### Sample Reference Data
```sql
SELECT brand_name, model, price, 5G_or_not, battery_capacity
FROM brandrep_silver.dim_products_ref
WHERE brand = 'apple'
LIMIT 5;
```

### Sentiment by Price Segment
```sql
SELECT 
    CASE 
        WHEN p.price < 20000 THEN 'Budget'
        WHEN p.price < 50000 THEN 'Midrange'
        ELSE 'Flagship'
    END as segment,
    AVG(s.sentiment_score) as avg_sentiment
FROM brandrep_silver.sentiment_analysis s
JOIN brandrep_silver.dim_products_ref p
    ON LOWER(s.brand_name) = p.brand 
    AND s.category = p.model
GROUP BY 1;
```

---

## 📚 Documentation

- **DEPLOYMENT_SUMMARY.md** - Complete deployment report
- **REF_DATA_UPDATE.md** - Technical implementation details
- **TESTING.md** - Comprehensive testing guide
- **README.md** - Module documentation

---

## 🎯 Success Checklist

- [x] ✅ Infrastructure deployed
- [x] ✅ Reference data uploaded
- [x] ✅ Schema verified (23 columns)
- [ ] ⏳ Bronze layer fixed
- [ ] ⏳ Silver-core job tested
- [ ] ⏳ Silver-sentiment job tested
- [ ] ⏳ End-to-end pipeline validated

---

**Status**: Ready for testing after Bronze layer fix  
**Contact**: Check logs in CloudWatch `/aws-glue/jobs/`

