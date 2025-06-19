# Banking Settlement Table Documentation

**Generated on:** 2025-06-17 13:04:14  
**Table Name:** `banking_settlement`  
**Model Class:** `BankingSettlement`

## Table Overview

The `banking_settlement` table is a critical component of the energy management system that manages multi-stage settlement processes for energy banking operations. It tracks surplus generation and demand through various settlement phases, providing a comprehensive audit trail of energy allocation and settlement calculations.

## Table Schema

### Primary Key
| Column | Data Type | Properties | Description |
|--------|-----------|------------|-------------|
| `id` | INTEGER | PRIMARY KEY, NOT NULL, AUTO_INCREMENT | Unique identifier for each settlement record |

### Identification & Context Fields
| Column | Data Type | Properties | Description |
|--------|-----------|------------|-------------|
| `client_name` | VARCHAR(255) | NOT NULL | Name of the client/customer |
| `plant_name` | VARCHAR(255) | NOT NULL | Name of the power generation plant |
| `date` | DATE | NOT NULL | Settlement date |
| `type` | ENUM('solar', 'wind') | NOT NULL | Type of power generation plant |
| `cons_unit` | VARCHAR(100) | NOT NULL | Consumption unit identifier |
| `slot_name` | VARCHAR(50) | NOT NULL | Time slot identifier (e.g., "Slot_1", "Slot_2") |
| `slot_time` | VARCHAR(20) | NOT NULL | Time slot value (e.g., "00:15", "00:30") |

### Stage 1: Initial Settlement Values
| Column | Data Type | Properties | Description |
|--------|-----------|------------|-------------|
| `surplus_demand_sum` | DECIMAL(12,2) | DEFAULT 0.00 | Total aggregated surplus demand before any settlements |
| `surplus_generation_sum` | DECIMAL(12,2) | DEFAULT 0.00 | Total aggregated surplus generation before any settlements |
| `matched_settled_sum` | DECIMAL(12,2) | DEFAULT 0.00 | Initial amount that could be directly matched and settled |

### Stage 2: Intra Settlement Fields
| Column | Data Type | Properties | Description |
|--------|-----------|------------|-------------|
| `surplus_generation_sum_after_intra` | DECIMAL(12,2) | DEFAULT 0.00 | Remaining surplus generation after intra-settlement |
| `surplus_demand_sum_after_intra` | DECIMAL(12,2) | DEFAULT 0.00 | Remaining surplus demand after intra-settlement |
| `intra_settlement` | DECIMAL(12,2) | DEFAULT 0.00 | Amount settled during intra-settlement process |

### Stage 3: Inter Settlement Fields
| Column | Data Type | Properties | Description |
|--------|-----------|------------|-------------|
| `surplus_generation_sum_after_inter` | DECIMAL(12,2) | DEFAULT 0.00 | Final remaining surplus generation after all settlements |
| `surplus_demand_sum_after_inter` | DECIMAL(12,2) | DEFAULT 0.00 | Final remaining surplus demand after all settlements |
| `inter_settlement` | DECIMAL(12,2) | DEFAULT 0.00 | Amount settled during inter-settlement process |

## Constraints

### Unique Constraints
- **uq_banking_stage**: Ensures uniqueness across the combination of:
  - `client_name`
  - `plant_name` 
  - `cons_unit`
  - `slot_name`
  - `date`
  - `type`

This constraint prevents duplicate settlement records for the same client, plant, consumption unit, time slot, and date combination.

## Settlement Process Flow

The banking settlement process follows a structured three-stage approach:

### Stage 1: Initial Aggregation
```
Purpose: Collect and aggregate initial surplus values
Process:
1. Calculate total surplus demand from consumption data
2. Calculate total surplus generation from production data  
3. Determine initial matched settlement amount
4. Store baseline values for further processing

Fields Updated:
- surplus_demand_sum
- surplus_generation_sum
- matched_settled_sum
```

### Stage 2: Intra Settlement
```
Purpose: Settle surplus within same client/plant/unit combinations
Process:
1. Match surplus generation with surplus demand within same entity
2. Calculate remaining surplus after intra-settlement
3. Record intra-settlement amounts

Fields Updated:
- surplus_generation_sum_after_intra
- surplus_demand_sum_after_intra  
- intra_settlement

Calculation Logic:
intra_settlement = min(surplus_generation_sum, surplus_demand_sum)
surplus_generation_sum_after_intra = surplus_generation_sum - intra_settlement
surplus_demand_sum_after_intra = surplus_demand_sum - intra_settlement
```

### Stage 3: Inter Settlement
```
Purpose: Settle remaining surplus across different entities
Process:
1. Match remaining surplus generation with surplus demand across entities
2. Calculate final surplus amounts
3. Record inter-settlement amounts

Fields Updated:
- surplus_generation_sum_after_inter
- surplus_demand_sum_after_inter
- inter_settlement

Calculation Logic:
inter_settlement = min(surplus_generation_sum_after_intra, surplus_demand_sum_after_intra)
surplus_generation_sum_after_inter = surplus_generation_sum_after_intra - inter_settlement
surplus_demand_sum_after_inter = surplus_demand_sum_after_intra - inter_settlement
```

## Business Rules & Logic

1. **Uniqueness**: Each record represents a unique combination of client, plant, consumption unit, time slot, date, and plant type
2. **Sequential Processing**: Settlement values must progress through three distinct stages in order
3. **Precision**: All monetary values are stored with 2 decimal precision for accuracy
4. **Default Values**: All settlement amounts default to 0.00 to ensure clean initialization
5. **Conservation**: Total energy should be conserved across all settlement stages
6. **Audit Trail**: Each stage maintains its values for complete traceability

## Data Relationships

### Related Tables
- **settlement_data**: Provides detailed transaction-level data that aggregates into banking_settlement
- **tbl_plants**: Referenced through plant_name for plant metadata
- **consumption_mapping**: Connected through cons_unit for consumption allocation
- **tbl_generation**: Source of generation surplus data
- **tbl_consumption**: Source of consumption surplus data

### Data Flow
```
tbl_generation + tbl_consumption → settlement_data → banking_settlement
                                       ↑
                                consumption_mapping
```

## Usage Patterns

### Insert Pattern
```sql
-- Initial record creation with Stage 1 values
INSERT INTO banking_settlement (
    client_name, plant_name, date, type, cons_unit, 
    slot_name, slot_time, surplus_demand_sum, 
    surplus_generation_sum, matched_settled_sum
) VALUES (...);
```

### Update Pattern - Stage 2
```sql
-- Update with intra-settlement calculations
UPDATE banking_settlement 
SET surplus_generation_sum_after_intra = ?,
    surplus_demand_sum_after_intra = ?,
    intra_settlement = ?
WHERE client_name = ? AND plant_name = ? AND date = ? AND cons_unit = ?;
```

### Update Pattern - Stage 3
```sql
-- Final update with inter-settlement results
UPDATE banking_settlement 
SET surplus_generation_sum_after_inter = ?,
    surplus_demand_sum_after_inter = ?,
    inter_settlement = ?
WHERE client_name = ? AND plant_name = ? AND date = ? AND cons_unit = ?;
```

### Query Patterns
```sql
-- Settlement summary by client and date
SELECT client_name, date, 
       SUM(surplus_generation_sum) as total_surplus_gen,
       SUM(surplus_demand_sum) as total_surplus_demand,
       SUM(intra_settlement + inter_settlement) as total_settled
FROM banking_settlement 
WHERE date BETWEEN ? AND ?
GROUP BY client_name, date;

-- Unsettled surplus analysis
SELECT client_name, plant_name, date,
       surplus_generation_sum_after_inter as unsettled_generation,
       surplus_demand_sum_after_inter as unsettled_demand
FROM banking_settlement 
WHERE surplus_generation_sum_after_inter > 0 
   OR surplus_demand_sum_after_inter > 0;
```

## Performance Considerations

1. **Indexing**: The unique constraint automatically creates an index on the key combination
2. **Partitioning**: Consider date-based partitioning for large datasets
3. **Batch Processing**: Settlement calculations should be performed in batches by date/client
4. **Archiving**: Old settlement data should be archived periodically

## Data Validation Rules

1. All surplus and settlement amounts must be non-negative
2. Settlement amounts cannot exceed available surplus
3. Stage progression: each stage's "after" values must be <= previous stage values
4. Date consistency: all related records must have valid date references
5. Enum validation: plant type must be either 'solar' or 'wind'

## Error Handling

- **Duplicate Key**: Handle unique constraint violations gracefully
- **Negative Values**: Validate all monetary amounts are >= 0
- **Missing References**: Ensure all foreign key relationships exist
- **Stage Violations**: Prevent out-of-order stage updates

## Monitoring & Alerting

Recommended monitoring points:
- Records with negative settlement values
- Large unsettled surplus amounts
- Missing settlement stages (incomplete records)
- Processing time anomalies
- Data consistency across related tables

---

**Note**: This documentation should be updated whenever the table structure or business logic changes. Regular reviews ensure accuracy and completeness of the settlement process documentation.