package clickhouse

import (
	"context"
	// "database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

//	func (ch *ClickHouseAdapter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
//		fmt.Println("req---  ", req)
//		commitDone := false
//		tx, err := ch.db.Begin()
//		if err != nil {
//			return 0, err
//		}
//		defer func() {
//			if !commitDone {
//				tx.Rollback()
//			}
//		}()
//		// NOTE: Value of ch.table is sanitized in NewClickHouseAdapter.
//		stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s (updated_at, metric_name, labels, value)", ch.table))
//		if err != nil {
//			return 0, err
//		}
//		defer stmt.Close()
//		count := 0
//		for _, t := range req.Timeseries {
//			var name string
//			labels := make([]string, 0, len(t.Labels))
//			// Note that label names are in sorted order per the remote write spec.
//			for _, l := range t.Labels {
//				if l.Name == "__name__" {
//					name = l.Value
//					continue
//				}
//				labels = append(labels, l.Name+"="+l.Value)
//			}
//			count += len(t.Samples)
//			for _, s := range t.Samples {
//				_, err = stmt.Exec(
//					time.UnixMilli(s.Timestamp).UTC(), // updated_at
//					name,                              // metric_name
//					labels,                            // labels
//					s.Value,                           // value
//				)
//				if err != nil {
//					return 0, err
//				}
//			}
//		}
//			// err = tx.Commit()
//			// commitDone = true
//			return count, err
//		}
func (ch *ClickHouseAdapter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
	commitDone := false

	tx, err := ch.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if !commitDone {
			_ = tx.Rollback()
		}
	}()

	// Adjust insert statement to match your schema
	// stmt, err := tx.PrepareContext(ctx,
	// 	fmt.Sprintf("INSERT INTO %s (updated_at, metric_name, value, instance, job, env) VALUES (?, ?, ?, ?, ?, ?)", ch.table))
	// if err != nil {
	// 	return 0, err
	// }
	// defer stmt.Close()

	// count := 0

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
		query := getInsertQuery(ch.databse_name, metricName, tableName)
		count := 0
		if query != "" {
			fmt.Println(query)
			stmt, err := tx.PrepareContext(ctx, query)
			if err != nil {
				return 0, err
			}
			defer stmt.Close()
			// stmtCache[metricName] = stmt
		} else {
			return 0, nil
		}
		// var name, instance, job, env string

		// for _, l := range t.Labels {
		// 	switch l.Name {
		// 	case "__name__":
		// 		name = l.Value
		// 	case "instance":
		// 		instance = l.Value
		// 	case "job":
		// 		job = l.Value
		// 	case "env":
		// 		env = l.Value
		// 	}
		// }

		for _, sample := range ts.Samples {

			fmt.Println("ts.Samples inside--", labelsMap)
			params := buildParams(sample, labelsMap, metricName)
			fmt.Println(params...)
			_, err := stmt.Exec(params...)
			if err != nil {
				return 0, err
			}

			// _, err := stmt.Exec(
			// 	time.UnixMilli(s.Timestamp).UTC(),
			// 	name,
			// 	s.Value,
			// 	instance,
			// 	job,
			// 	env,
			// )
			// if err != nil {
			// 	return 0, err
			// }
			count++
		}
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}
	commitDone = true

	return count, nil
}

// func (ch *ClickHouseAdapter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
// 	tx, err := ch.db.Begin()
// 	if err != nil {
// 		return 0, err
// 	}
// 	commitDone := false
// 	defer func() {
// 		if !commitDone {
// 			_ = tx.Rollback()
// 		}
// 	}()
// 	defer func() {
// 		if err != nil {
// 			fmt.Printf("Rolling back due to error: %v", err)
// 			fmt.Println()
// 			_ = tx.Rollback()
// 		} else {
// 			err = tx.Commit()
// 			if err != nil {
// 				fmt.Printf("Failed to commit transaction: %v", err)
// 				fmt.Println()
// 			}
// 		}
// 	}()

// 	stmtCache := make(map[string]*sql.Stmt)
// 	batchCache := make(map[string][]interface{})
// 	defer func() {
// 		for _, stmt := range stmtCache {
// 			_ = stmt.Close()
// 		}
// 	}()

// 	count := 0
// 	batchSize := 100

// 	flushBatch := func(metric string) error {
// 		stmt := stmtCache[metric]
// 		batch := batchCache[metric]
// 		for i := 0; i < len(batch); i += 10 {
// 			end := i + 10
// 			if end > len(batch) {
// 				end = len(batch)
// 			}
// 			if _, err := stmt.Exec(batch[i:end]...); err != nil {
// 				return err
// 			}
// 		}
// 		batchCache[metric] = nil
// 		return nil
// 	}

// 	for _, ts := range req.Timeseries {
// 		var metricName string
// 		labelsMap := make(map[string]string)
// 		for _, label := range ts.Labels {
// 			if label.Name == "__name__" {
// 				metricName = label.Value
// 			} else {
// 				labelsMap[label.Name] = label.Value
// 			}
// 		}

// 		tableName := fmt.Sprintf("metrics_%s", metricName)
// 		if _, ok := stmtCache[metricName]; !ok {
// 			query := getInsertQuery(ch.databse_name, metricName, tableName)

// 			if query != "" {
// 				fmt.Println(query)
// 				stmt, err := tx.PrepareContext(ctx, query)
// 				if err != nil {
// 					return 0, err
// 				}
// 				stmtCache[metricName] = stmt
// 			} else {
// 				return 0, nil
// 			}

// 		}
// 		fmt.Println("ts.Samples--", ts.Samples)
// 		for _, sample := range ts.Samples {
// 			fmt.Println("ts.Samples inside--", labelsMap)
// 			params := buildParams(sample, labelsMap, metricName)
// 			fmt.Println(params...)
// 			batchCache[metricName] = append(batchCache[metricName], params...)
// 			count++

// 			if len(batchCache[metricName])/10 >= batchSize {
// 				if err := flushBatch(metricName); err != nil {
// 					return 0, err
// 				}
// 			}
// 		}
// 	}

// 	// Flush remaining
// 	for metric := range batchCache {
// 		if len(batchCache[metric]) > 0 {
// 			if err := flushBatch(metric); err != nil {
// 				return 0, err
// 			}
// 		}
// 	}

// 	if err := tx.Commit(); err != nil {
// 		return 0, err
// 	}
// 	commitDone = true
// 	return count, nil
// }

func getInsertQuery(db, metric string, tableName string) string {

	switch metric {
	case "hwAvgDuty5min":
		fmt.Println(db, metric, tableName)
		// tx.PrepareContext(ctx,
		// fmt.Sprintf("INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, hwCpuDevIndex, hwFrameIndex, hwSlotIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", db, tableName))
		return fmt.Sprintf("INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, hwCpuDevIndex, hwFrameIndex, hwSlotIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", db, tableName)
	case "hwMemoryDevFree":
		fmt.Println(db, metric, tableName)
		return fmt.Sprintf("INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, hwMemoryDevModuleIndex, hwFrameIndex, hwSlotIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", db, tableName)
	case "ifAlias":
		fmt.Println(db, metric, tableName)
		return fmt.Sprintf("INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, ifAlias, ifIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", db, tableName)
	case "ifDescr":
		fmt.Println(db, metric, tableName)
		return fmt.Sprintf("INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, ifDescr, ifIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", db, tableName)
	case "ifName":
		fmt.Println(db, metric, tableName)
		return fmt.Sprintf("INSERT INTO %s.%s (updated_at, value, instance, job, auth, env, ifName, ifIndex, module) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", db, tableName)
	default:
		return ""
	}
}

func buildParams(sample prompb.Sample, labels map[string]string, metricName string) []interface{} {
	fmt.Println("buildParams", metricName, "Row values:")
	t := time.UnixMilli(sample.Timestamp).UTC()
	v := sample.Value
	switch metricName {
	case "hwAvgDuty5min":
		row := []interface{}{t, v, labels["instance"], labels["job"], labels["auth"], labels["env"], parseFloat(labels["hwCpuDevIndex"]), parseFloat(labels["hwFrameIndex"]), parseFloat(labels["hwSlotIndex"]), labels["module"]}
		fmt.Println("buildParams", metricName, "Row values:", row)
		return row
	case "hwMemoryDevFree":
		row := []interface{}{t, v, labels["instance"], labels["job"], labels["auth"], labels["env"], parseFloat(labels["hwMemoryDevModuleIndex"]), parseFloat(labels["hwFrameIndex"]), parseFloat(labels["hwSlotIndex"]), labels["module"]}
		fmt.Println("buildParams", metricName, "Row values:", row)
		return row
	case "ifAlias":
		row := []interface{}{t, v, labels["instance"], labels["job"], labels["auth"], labels["env"], labels["ifAlias"], parseFloat(labels["ifIndex"]), labels["module"]}
		fmt.Println("buildParams", metricName, "Row values:", row)
		return row
	case "ifDescr":
		row := []interface{}{t, v, labels["instance"], labels["job"], labels["auth"], labels["env"], labels["ifDescr"], parseFloat(labels["ifIndex"]), labels["module"]}
		fmt.Println("buildParams", metricName, "Row values:", row)
		return row
	case "ifName":
		row := []interface{}{t, v, labels["instance"], labels["job"], labels["auth"], labels["env"], labels["ifName"], parseFloat(labels["ifIndex"]), labels["module"]}
		fmt.Println("buildParams", metricName, "Row values:", row)
		return row
	default:
		return nil
	}
}

func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
