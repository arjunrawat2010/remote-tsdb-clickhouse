package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

// func (ch *ClickHouseAdapter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {

// 	fmt.Println("req---  ", req)
// 	commitDone := false

// 	tx, err := ch.db.Begin()
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer func() {
// 		if !commitDone {
// 			tx.Rollback()
// 		}
// 	}()

// 	// NOTE: Value of ch.table is sanitized in NewClickHouseAdapter.
// 	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s (updated_at, metric_name, labels, value)", ch.table))
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer stmt.Close()

// 	count := 0

// 	for _, t := range req.Timeseries {
// 		var name string
// 		labels := make([]string, 0, len(t.Labels))

// 		// Note that label names are in sorted order per the remote write spec.
// 		for _, l := range t.Labels {
// 			if l.Name == "__name__" {
// 				name = l.Value
// 				continue
// 			}
// 			labels = append(labels, l.Name+"="+l.Value)
// 		}

// 		count += len(t.Samples)
// 		for _, s := range t.Samples {
// 			_, err = stmt.Exec(
// 				time.UnixMilli(s.Timestamp).UTC(), // updated_at
// 				name,                              // metric_name
// 				labels,                            // labels
// 				s.Value,                           // value
// 			)
// 			if err != nil {
// 				return 0, err
// 			}
// 		}
// 	}

//		// err = tx.Commit()
//		// commitDone = true
//		return count, err
//	}
// func (ch *ClickHouseAdapter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
// 	commitDone := false

// 	tx, err := ch.db.Begin()
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer func() {
// 		if !commitDone {
// 			_ = tx.Rollback()
// 		}
// 	}()

// 	// Adjust insert statement to match your schema
// 	stmt, err := tx.PrepareContext(ctx,
// 		fmt.Sprintf("INSERT INTO %s (updated_at, metric_name, value, instance, job, env) VALUES (?, ?, ?, ?, ?, ?)", ch.table))
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer stmt.Close()

// 	count := 0

// 	for _, t := range req.Timeseries {
// 		var name, instance, job, env string

// 		for _, l := range t.Labels {
// 			switch l.Name {
// 			case "__name__":
// 				name = l.Value
// 			case "instance":
// 				instance = l.Value
// 			case "job":
// 				job = l.Value
// 			case "env":
// 				env = l.Value
// 			}
// 		}

// 		for _, s := range t.Samples {
// 			_, err := stmt.Exec(
// 				time.UnixMilli(s.Timestamp).UTC(),
// 				name,
// 				s.Value,
// 				instance,
// 				job,
// 				env,
// 			)
// 			if err != nil {
// 				return 0, err
// 			}
// 			count++
// 		}
// 	}

// 	err = tx.Commit()
// 	if err != nil {
// 		return 0, err
// 	}
// 	commitDone = true

//		return count, nil
//	}
// func (ch *ClickHouseAdapter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
// 	tx, err := ch.db.Begin()
// 	// fmt.Println("database---", ch.databse_name)
// 	if err != nil {
// 		return 0, err
// 	}
// 	commitDone := false
// 	defer func() {
// 		if !commitDone {
// 			_ = tx.Rollback()
// 		}
// 	}()

// 	stmtCache := make(map[string]*sql.Stmt)
// 	defer func() {
// 		for _, stmt := range stmtCache {
// 			_ = stmt.Close()
// 		}
// 	}()

// 	count := 0

// 	for _, ts := range req.Timeseries {
// 		// fmt.Println("1--ts----", ts)
// 		// fmt.Println("ts----", ts.Labels)
// 		var metricName string
// 		labelsMap := make(map[string]string)

// 		for _, label := range ts.Labels {
// 			// fmt.Println("2--label Name----", label.Name, "-- value --", label.Value)
// 			if label.Name == "__name__" {
// 				metricName = label.Value
// 			} else {
// 				labelsMap[label.Name] = label.Value
// 			}
// 			// fmt.Println("3--label----", label)
// 		}
// 		// fmt.Println("4--labelsMap----", labelsMap)
// 		// fmt.Println("5 -- instance --",
// 		// 	labelsMap["instance"], "-- job --",
// 		// 	labelsMap["job"], "-- auth --",
// 		// 	labelsMap["auth"], "-- env --",
// 		// 	labelsMap["env"])
// 		// Define target table name (e.g., "metrics_<metricName>")
// 		tableName := fmt.Sprintf("metrics_%s", metricName)

// 		// Only support known metrics with fixed schemas
// 		switch metricName {
// 		case "hwAvgDuty5min":
// 			stmt, ok := stmtCache[tableName]
// 			if !ok {
// 				stmt, err = tx.PrepareContext(ctx, fmt.Sprintf(
// 					"INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, hwCpuDevIndex, hwFrameIndex, hwSlotIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ch.databse_name, tableName))
// 				if err != nil {
// 					return 0, err
// 				}
// 				stmtCache[tableName] = stmt
// 			}
// 			for _, sample := range ts.Samples {

// 				hwCpuDevIndex, hwCpuDevIndex_err := strconv.ParseFloat(labelsMap["hwCpuDevIndex"], 64)
// 				if hwCpuDevIndex_err != nil {
// 					fmt.Printf("Invalid float64 value for hwCpuDevIndex: %v (error: %v)\n", hwCpuDevIndex, hwCpuDevIndex_err)
// 					// Handle the error (e.g., skip or insert default value)
// 				}
// 				hwFrameIndex, hwFrameIndex_err := strconv.ParseFloat(labelsMap["hwFrameIndex"], 64)
// 				if hwFrameIndex_err != nil {
// 					fmt.Printf("Invalid float64 value for hwFrameIndex: %v (error: %v)\n", hwFrameIndex, hwFrameIndex_err)
// 					// Handle the error (e.g., skip or insert default value)
// 				}
// 				hwSlotIndex, hwSlotIndex_err := strconv.ParseFloat(labelsMap["hwSlotIndex"], 64)
// 				if hwSlotIndex_err != nil {
// 					fmt.Printf("Invalid float64 value for hwSlotIndex: %v (error: %v)\n", hwSlotIndex, hwSlotIndex_err)
// 					// Handle the error (e.g., skip or insert default value)
// 				}
// 				result, err := stmt.Exec(
// 					time.UnixMilli(sample.Timestamp).UTC(),
// 					sample.Value,
// 					labelsMap["instance"],
// 					labelsMap["job"],
// 					labelsMap["auth"],
// 					labelsMap["env"],
// 					hwCpuDevIndex,
// 					hwFrameIndex,
// 					hwSlotIndex,
// 					labelsMap["module"],
// 				)
// 				if err != nil {
// 					return 0, err
// 				} else {
// 					rowsAffected, _ := result.RowsAffected()
// 					lastInsertId, _ := result.LastInsertId() // May not be supported by ClickHouse driver
// 					resultLog := fmt.Sprintf("9--%s Insert successful: %d rows affected, last insert ID: %d", tableName, rowsAffected, lastInsertId)
// 					fmt.Println(resultLog)
// 				}
// 				count++
// 				if err := tx.Commit(); err != nil {
// 					return 0, err
// 				}
// 				commitDone = true
// 			}
// 		case "hwMemoryDevFree":
// 			stmt, ok := stmtCache[tableName]
// 			if !ok {
// 				stmt, err = tx.PrepareContext(ctx, fmt.Sprintf(
// 					"INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, hwMemoryDevModuleIndex, hwFrameIndex, hwSlotIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ch.databse_name, tableName))
// 				if err != nil {
// 					return 0, err
// 				}
// 				stmtCache[tableName] = stmt
// 			}
// 			for _, sample := range ts.Samples {

// 				hwMemoryDevModuleIndex, hwMemoryDevModuleIndex_err := strconv.ParseFloat(labelsMap["hwMemoryDevModuleIndex"], 64)
// 				if hwMemoryDevModuleIndex_err != nil {
// 					fmt.Printf("Invalid float64 value for hwMemoryDevModuleIndex: %v (error: %v)\n", hwMemoryDevModuleIndex, hwMemoryDevModuleIndex_err)
// 					// Handle the error (e.g., skip or insert default value)
// 				}
// 				hwFrameIndex, hwFrameIndex_err := strconv.ParseFloat(labelsMap["hwFrameIndex"], 64)
// 				if hwFrameIndex_err != nil {
// 					fmt.Printf("Invalid float64 value for hwFrameIndex: %v (error: %v)\n", hwFrameIndex, hwFrameIndex_err)
// 					// Handle the error (e.g., skip or insert default value)
// 				}
// 				hwSlotIndex, hwSlotIndex_err := strconv.ParseFloat(labelsMap["hwSlotIndex"], 64)
// 				if hwSlotIndex_err != nil {
// 					fmt.Printf("Invalid float64 value for hwSlotIndex: %v (error: %v)\n", hwSlotIndex, hwSlotIndex_err)
// 					// Handle the error (e.g., skip or insert default value)
// 				}
// 				result, err := stmt.Exec(
// 					time.UnixMilli(sample.Timestamp).UTC(),
// 					sample.Value,
// 					labelsMap["instance"],
// 					labelsMap["job"],
// 					labelsMap["auth"],
// 					labelsMap["env"],
// 					hwMemoryDevModuleIndex,
// 					hwFrameIndex,
// 					hwSlotIndex,
// 					labelsMap["module"],
// 				)
// 				if err != nil {
// 					return 0, err
// 				} else {
// 					rowsAffected, _ := result.RowsAffected()
// 					lastInsertId, _ := result.LastInsertId() // May not be supported by ClickHouse driver
// 					resultLog := fmt.Sprintf("9--%s Insert successful: %d rows affected, last insert ID: %d", tableName, rowsAffected, lastInsertId)
// 					fmt.Println(resultLog)
// 				}
// 				count++
// 				if err := tx.Commit(); err != nil {
// 					return 0, err
// 				}
// 				commitDone = true
// 			}
// 		case "ifAlias":
// 			stmt, ok := stmtCache[tableName]
// 			// fmt.Println("stmtCache", stmtCache)
// 			// fmt.Println("stmtCache ta--", stmtCache[tableName])
// 			if !ok {
// 				stmt, err = tx.PrepareContext(ctx, fmt.Sprintf(
// 					"INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, ifAlias, ifIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ch.databse_name, tableName))
// 				if err != nil {
// 					return 0, err
// 				}
// 				stmtCache[tableName] = stmt
// 				// fmt.Println("6--stmt", stmt)
// 				// fmt.Println("7--Lebels Map --", labelsMap)
// 			}
// 			for _, sample := range ts.Samples {

// 				ifIndex, ifIndex_err := strconv.ParseFloat(labelsMap["ifIndex"], 64)
// 				if ifIndex_err != nil {
// 					fmt.Printf("Invalid float64 value for ifIndex: %v (error: %v)\n", ifIndex, ifIndex_err)
// 					// Handle the error (e.g., skip or insert default value)
// 				}
// 				ifAlias := labelsMap["ifAlias"]
// 				if ifAlias == "" {
// 					ifAlias = ""
// 				}
// 				// queryDebug := fmt.Sprintf(
// 				// 	"INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, ifAlias, ifIndex, module) VALUES ('%s', %f, '%s', '%s', '%s', '%s', '%s', %f, '%s')",
// 				// 	ch.databse_name,
// 				// 	tableName,
// 				// 	time.UnixMilli(sample.Timestamp).UTC().Format("2006-01-02 15:04:05"),
// 				// 	sample.Value,
// 				// 	labelsMap["instance"],
// 				// 	labelsMap["job"],
// 				// 	labelsMap["auth"],
// 				// 	labelsMap["env"],
// 				// 	labelsMap["ifAlias"],
// 				// 	ifIndex,
// 				// 	labelsMap["module"],
// 				// )

// 				// fmt.Println("Executing SQL:", queryDebug)

// 				result, err := stmt.Exec(
// 					time.UnixMilli(sample.Timestamp).UTC(),
// 					sample.Value,
// 					labelsMap["instance"],
// 					labelsMap["job"],
// 					labelsMap["auth"],
// 					labelsMap["env"],
// 					labelsMap["ifAlias"],
// 					ifIndex,
// 					labelsMap["module"],
// 				)
// 				if err != nil {
// 					return 0, err
// 				} else {
// 					// fmt.Println("result --", result)
// 					rowsAffected, _ := result.RowsAffected()
// 					lastInsertId, _ := result.LastInsertId() // May not be supported by ClickHouse driver
// 					resultLog := fmt.Sprintf("9--%s Insert successful: %d rows affected, last insert ID: %d", tableName, rowsAffected, lastInsertId)
// 					fmt.Println(resultLog)
// 				}
// 				count++
// 				// if err := tx.Commit(); err != nil {
// 				// 	return 0, err
// 				// }
// 				// commitDone = true
// 			}
// 		case "ifDescr":
// 			stmt, ok := stmtCache[tableName]
// 			if !ok {
// 				stmt, err = tx.PrepareContext(ctx, fmt.Sprintf(
// 					"INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, ifDescr, ifIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ch.databse_name, tableName))
// 				if err != nil {
// 					return 0, err
// 				}
// 				stmtCache[tableName] = stmt
// 			}
// 			for _, sample := range ts.Samples {

// 				ifIndex, ifIndex_err := strconv.ParseFloat(labelsMap["ifIndex"], 64)
// 				if ifIndex_err != nil {
// 					fmt.Printf("Invalid float64 value for ifIndex: %v (error: %v)\n", ifIndex, ifIndex_err)
// 					// Handle the error (e.g., skip or insert default value)
// 				}
// 				result, err := stmt.Exec(
// 					time.UnixMilli(sample.Timestamp).UTC(),
// 					sample.Value,
// 					labelsMap["instance"],
// 					labelsMap["job"],
// 					labelsMap["auth"],
// 					labelsMap["env"],
// 					labelsMap["ifDescr"],
// 					ifIndex,
// 					labelsMap["module"],
// 				)
// 				if err != nil {
// 					return 0, err
// 				} else {
// 					rowsAffected, _ := result.RowsAffected()
// 					lastInsertId, _ := result.LastInsertId() // May not be supported by ClickHouse driver
// 					resultLog := fmt.Sprintf("9--%s Insert successful: %d rows affected, last insert ID: %d", tableName, rowsAffected, lastInsertId)
// 					fmt.Println(resultLog)
// 				}
// 				count++
// 				if err := tx.Commit(); err != nil {
// 					return 0, err
// 				}
// 				commitDone = true
// 			}
// 		case "ifName":
// 			stmt, ok := stmtCache[tableName]
// 			if !ok {
// 				stmt, err = tx.PrepareContext(ctx, fmt.Sprintf(
// 					"INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, ifName, ifIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ch.databse_name, tableName))
// 				if err != nil {
// 					return 0, err
// 				}
// 				stmtCache[tableName] = stmt
// 			}
// 			for _, sample := range ts.Samples {

// 				ifIndex, ifIndex_err := strconv.ParseFloat(labelsMap["ifIndex"], 64)
// 				if ifIndex_err != nil {
// 					fmt.Printf("Invalid float64 value for ifIndex: %v (error: %v)\n", ifIndex, ifIndex_err)
// 					// Handle the error (e.g., skip or insert default value)
// 				}
// 				result, err := stmt.Exec(
// 					time.UnixMilli(sample.Timestamp).UTC(),
// 					sample.Value,
// 					labelsMap["instance"],
// 					labelsMap["job"],
// 					labelsMap["auth"],
// 					labelsMap["env"],
// 					labelsMap["ifName"],
// 					ifIndex,
// 					labelsMap["module"],
// 				)
// 				if err != nil {
// 					return 0, err
// 				} else {
// 					rowsAffected, _ := result.RowsAffected()
// 					lastInsertId, _ := result.LastInsertId() // May not be supported by ClickHouse driver
// 					resultLog := fmt.Sprintf("9--%s Insert successful: %d rows affected, last insert ID: %d", tableName, rowsAffected, lastInsertId)
// 					fmt.Println(resultLog)
// 				}
// 				count++
// 				if err := tx.Commit(); err != nil {
// 					return 0, err
// 				}
// 				commitDone = true
// 			}
// 		case "sysDescr":
// 			stmt, ok := stmtCache[tableName]
// 			if !ok {
// 				stmt, err = tx.PrepareContext(ctx, fmt.Sprintf(
// 					"INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, sysDescr, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ch.databse_name, tableName))
// 				if err != nil {
// 					return 0, err
// 				}
// 				stmtCache[tableName] = stmt
// 			}
// 			for _, sample := range ts.Samples {

// 				// ifIndex, ifIndex_err := strconv.ParseFloat(labelsMap["ifIndex"], 64)
// 				// if ifIndex_err != nil {
// 				// 	fmt.Printf("Invalid float64 value for ifIndex: %v (error: %v)\n", ifIndex, ifIndex_err)
// 				// 	// Handle the error (e.g., skip or insert default value)
// 				// }
// 				result, err := stmt.Exec(
// 					time.UnixMilli(sample.Timestamp).UTC(),
// 					sample.Value,
// 					labelsMap["instance"],
// 					labelsMap["job"],
// 					labelsMap["auth"],
// 					labelsMap["env"],
// 					labelsMap["sysDescr"],
// 					labelsMap["module"],
// 				)
// 				if err != nil {
// 					return 0, err
// 				} else {
// 					rowsAffected, _ := result.RowsAffected()
// 					lastInsertId, _ := result.LastInsertId() // May not be supported by ClickHouse driver
// 					resultLog := fmt.Sprintf("9--%s Insert successful: %d rows affected, last insert ID: %d", tableName, rowsAffected, lastInsertId)
// 					fmt.Println(resultLog)
// 				}
// 				count++
// 				if err := tx.Commit(); err != nil {
// 					return 0, err
// 				}
// 				commitDone = true
// 			}
// 		case "ifAdminStatus":
// 		case "ifConnectorPresent":
// 		case "ifCounterDiscontinuityTime":
// 		case "ifHCInOctets":
// 		case "ifHCOutOctets":
// 		case "ifHighSpeed":
// 		case "ifIndex":
// 		case "ifInErrors":
// 		case "ifLastChange":
// 		case "ifLinkUpDownTrapEnable":
// 		case "ifMtu":
// 		case "ifOperStatus":
// 		case "ifSpeed":
// 		case "ifType":
// 			stmt, ok := stmtCache[tableName]
// 			if !ok {
// 				stmt, err = tx.PrepareContext(ctx, fmt.Sprintf(
// 					"INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, ifIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ch.databse_name, tableName))
// 				if err != nil {
// 					return 0, err
// 				}
// 				stmtCache[tableName] = stmt
// 			}
// 			for _, sample := range ts.Samples {

// 				ifIndex, ifIndex_err := strconv.ParseFloat(labelsMap["ifIndex"], 64)
// 				if ifIndex_err != nil {
// 					fmt.Printf("Invalid float64 value for ifIndex: %v (error: %v)\n", ifIndex, ifIndex_err)
// 					// Handle the error (e.g., skip or insert default value)
// 				}
// 				result, err := stmt.Exec(
// 					time.UnixMilli(sample.Timestamp).UTC(),
// 					sample.Value,
// 					labelsMap["instance"],
// 					labelsMap["job"],
// 					labelsMap["auth"],
// 					labelsMap["env"],
// 					ifIndex,
// 					labelsMap["module"],
// 				)
// 				if err != nil {
// 					return 0, err
// 				} else {
// 					rowsAffected, _ := result.RowsAffected()
// 					lastInsertId, _ := result.LastInsertId() // May not be supported by ClickHouse driver
// 					resultLog := fmt.Sprintf("9--%s Insert successful: %d rows affected, last insert ID: %d", tableName, rowsAffected, lastInsertId)
// 					fmt.Println(resultLog)
// 				}
// 				count++
// 				if err := tx.Commit(); err != nil {
// 					return 0, err
// 				}
// 				commitDone = true
// 			}

// 		default:
// 			// Unknown metric — skip or log
// 			//fmt.Printf("Skipping unknown metric: %s\n", metricName)
// 		}
// 	}

// 	if err := tx.Commit(); err != nil {
// 		return 0, err
// 	}
// 	commitDone = true
// 	// if err := tx.Commit(); err != nil {
// 	// 	return 0, err
// 	// }

// 	return count, nil
// }

func (ch *ClickHouseAdapter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
	tx, err := ch.db.Begin()
	if err != nil {
		return 0, err
	}
	commitDone := false
	defer func() {
		if !commitDone {
			_ = tx.Rollback()
		}
	}()

	stmtCache := make(map[string]*sql.Stmt)
	batchCache := make(map[string][]interface{})
	defer func() {
		for _, stmt := range stmtCache {
			_ = stmt.Close()
		}
	}()

	count := 0
	batchSize := 100

	flushBatch := func(metric string) error {
		stmt := stmtCache[metric]
		batch := batchCache[metric]
		for i := 0; i < len(batch); i += 10 {
			end := i + 10
			if end > len(batch) {
				end = len(batch)
			}
			if _, err := stmt.Exec(batch[i:end]...); err != nil {
				return err
			}
		}
		batchCache[metric] = nil
		return nil
	}

	for _, ts := range req.Timeseries {
		var metricName string
		labelsMap := make(map[string]string)
		for _, label := range ts.Labels {
			if label.Name == "__name__" {
				metricName = label.Value
			} else {
				labelsMap[label.Name] = label.Value
			}
		}

		tableName := fmt.Sprintf("metrics_%s", metricName)
		if _, ok := stmtCache[metricName]; !ok {
			query := getInsertQuery(ch.databse_name, metricName, tableName)
			fmt.Println(query)
			stmt, err := tx.PrepareContext(ctx, query)
			if err != nil {
				return 0, err
			}
			stmtCache[metricName] = stmt
		}

		for _, sample := range ts.Samples {
			params := buildParams(sample, labelsMap)
			fmt.Println(params...)
			batchCache[metricName] = append(batchCache[metricName], params...)
			count++

			if len(batchCache[metricName])/10 >= batchSize {
				if err := flushBatch(metricName); err != nil {
					return 0, err
				}
			}
		}
	}

	// Flush remaining
	for metric := range batchCache {
		if len(batchCache[metric]) > 0 {
			if err := flushBatch(metric); err != nil {
				return 0, err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	commitDone = true
	return count, nil
}

func getInsertQuery(db, metric string, tableName string) string {
	switch metric {
	case "hwAvgDuty5min":
		return fmt.Sprintf("INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, hwCpuDevIndex, hwFrameIndex, hwSlotIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", db, tableName)
	case "hwMemoryDevFree":
		return fmt.Sprintf("INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, hwMemoryDevModuleIndex, hwFrameIndex, hwSlotIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", db, tableName)
	case "ifAlias":
		return fmt.Sprintf("INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, ifAlias, ifIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", db, tableName)
	case "ifDescr":
		return fmt.Sprintf("INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, ifDescr, ifIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", db, tableName)
	case "ifName":
		return fmt.Sprintf("INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, ifName, ifIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", db, tableName)
	default:
		return ""
	}
}

func buildParams(sample prompb.Sample, labels map[string]string) []interface{} {
	t := time.UnixMilli(sample.Timestamp).UTC()
	v := sample.Value
	switch labels["__name__"] {
	case "hwAvgDuty5min":
		return []interface{}{t, v, labels["instance"], labels["job"], labels["auth"], labels["env"], parseFloat(labels["hwCpuDevIndex"]), parseFloat(labels["hwFrameIndex"]), parseFloat(labels["hwSlotIndex"]), labels["module"]}
	case "hwMemoryDevFree":
		return []interface{}{t, v, labels["instance"], labels["job"], labels["auth"], labels["env"], parseFloat(labels["hwMemoryDevModuleIndex"]), parseFloat(labels["hwFrameIndex"]), parseFloat(labels["hwSlotIndex"]), labels["module"]}
	case "ifAlias":
		return []interface{}{t, v, labels["instance"], labels["job"], labels["auth"], labels["env"], labels["ifAlias"], parseFloat(labels["ifIndex"]), labels["module"]}
	case "ifDescr":
		return []interface{}{t, v, labels["instance"], labels["job"], labels["auth"], labels["env"], labels["ifDescr"], parseFloat(labels["ifIndex"]), labels["module"]}
	case "ifName":
		return []interface{}{t, v, labels["instance"], labels["job"], labels["auth"], labels["env"], labels["ifName"], parseFloat(labels["ifIndex"]), labels["module"]}
	default:
		return nil
	}
}

func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
