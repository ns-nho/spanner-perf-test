package database

import (
	"cloud.google.com/go/spanner"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"google.golang.org/api/iterator"
	"hash/fnv"
	"time"
)

const sqlNewQueryPolicies = `SELECT t1.policy_id, t1.policy_cfg, t2.policies_version
FROM policies AS t1 JOIN tenants AS t2 ON t1.tenant_id = t2.tenant_id AND t1.hash_id = t2.hash_id
WHERE t1.tenant_id = @tenant_id and t1.hash_id = @hash_id
AND (@last_policy_id < 0 OR t1.policy_id > @last_policy_id)
ORDER BY t1.policy_id
LIMIT 1000`

const sqlNewQueryPrivateApps = `SELECT t1.app_id, t1.private_apps_cfg, t2.private_apps_version
FROM private_apps AS t1 JOIN tenants AS t2 ON t1.tenant_id = t2.tenant_id AND t1.hash_id = t2.hash_id
WHERE t1.tenant_id = @tenant_id AND t1.hash_id = @hash_id
AND (@last_app_id < 0 or t1.app_id > @last_app_id)
ORDER BY t1.app_id`

//const sqlNewQueryPoliciesWithoutPagination = `SELECT t1.policy_id, t1.policy_cfg, t2.policies_version
//FROM policies AS t1 JOIN tenants AS t2 ON t1.tenant_id = t2.tenant_id AND t1.hash_id = t2.hash_id
//WHERE t1.tenant_id = @tenant_id and t1.hash_id = @hash_id
//ORDER BY t1.policy_id`

//const sqlOldQueryPolicies = `SELECT t1.policy_id, t1.policy_cfg, t2.policies_version
//FROM policies AS t1 JOIN tenants AS t2 ON t1.tenant_id = t2.tenant_id
//WHERE t1.tenant_id = @tenant_id
//AND (@last_policy_id < 0 OR t1.policy_id > @last_policy_id)
//ORDER BY t1.policy_id
//LIMIT @page_size`

func QueryByPrimaryKey(tenantID uint64, lastAppID int64) (int, int64, error) {
	fmt.Printf("starting query tenant: %d\n", tenantID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := spanner.NewClient(ctx, url("npa-cspanner-prod", "npa-tenant-info", "tenant-info"))
	//client, err := spanner.NewClient(ctx, url("npa-cspanner-poc", "npa-fr4", "npa_srp_info"))
	//client, err := spanner.NewClient(ctx, url("npa-cspanner-poc", "npa-fr4", "npa_qa01_srp_info"))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create spanner client: %w", err)
	}
	defer client.Close()

	hashID, err := generateHashID(tenantID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to generate hash ID: %w", err)
	}

	statement := spanner.Statement{
		SQL: sqlNewQueryPrivateApps,
		//Params: map[string]interface{}{"tenant_id": int64(tenantID), "hash_id": int64(hashID), "last_policy_id": lastAppID},
		Params: map[string]interface{}{"tenant_id": int64(tenantID), "hash_id": int64(hashID), "last_app_id": lastAppID},
		//Params: map[string]interface{}{"tenant_id": int64(tenantID), "hash_id": int64(hashID)},
	}

	iter := client.Single().Query(ctx, statement)
	defer iter.Stop()

	count := 0
	var resultAppID int64
	var resultAppCfg string
	var resultAppVersion time.Time
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return 0, 0, fmt.Errorf("failed to execute query for tenant %d: %w", tenantID, err)
		}

		count++

		if err := row.Columns(&resultAppID, &resultAppCfg, &resultAppVersion); err != nil {
			return 0, 0, fmt.Errorf("failed to read row: %w", err)
		}

		fmt.Println(resultAppID, resultAppCfg, resultAppVersion)
	}
	nextPageToken := int64(-1)
	if count == 1000 {
		nextPageToken = resultAppID
	}
	return count, nextPageToken, nil
}

//func QueryByTenantID(tenantID uint64, lastPolicyID int64) (int, int64, error) {
//	fmt.Printf("starting query tenant: %d\n", tenantID)
//	ctx := context.Background()
//	client, err := spanner.NewClient(ctx, url("npa-cspanner-prod", "npa-tenant-info", "tenant-info"))
//	if err != nil {
//		return 0, 0, fmt.Errorf("failed to create spanner client: %w", err)
//	}
//	defer client.Close()
//
//	statement := spanner.Statement{
//		SQL:    sqlOldQueryPolicies,
//		Params: map[string]interface{}{"tenant_id": int64(tenantID), "last_policy_id": lastPolicyID, "page_size": 50},
//	}
//
//	iter := client.Single().Query(ctx, statement)
//	defer iter.Stop()
//
//	count := 0
//	var resultPolicyID int64
//	var resultPolicyCfg string
//	var resultPoliciesVersion time.Time
//	for {
//
//		row, err := iter.Next()
//		if errors.Is(err, iterator.Done) {
//			break
//		}
//		if err != nil {
//			return 0, 0, fmt.Errorf("failed to execute query for tenant %d: %w", tenantID, err)
//		}
//
//		count++
//
//		if err := row.Columns(&resultPolicyID, &resultPolicyCfg, &resultPoliciesVersion); err != nil {
//			return 0, 0, fmt.Errorf("failed to read row: %w", err)
//		}
//
//		fmt.Println(resultPolicyID, resultPolicyCfg, resultPoliciesVersion)
//	}
//	nextPageToken := int64(-1)
//	if count == 50 {
//		nextPageToken = resultPolicyID
//	}
//	return count, nextPageToken, nil
//}

//func BulkInsert(num int) error {
//	ctx := context.Background()
//	client, err := spanner.NewClient(ctx, url("perf-project", "perf-instance", "perf-database"))
//	if err != nil {
//		return fmt.Errorf("failed to create spanner client: %w", err)
//	}
//	defer client.Close()
//
//	mutations := make([]*spanner.Mutation, num)
//	for i := 0; i < 10000; i++ {
//		tenantID := uint64(i + 1)
//		hashID, err := generateHashID(tenantID)
//		if err != nil {
//			return fmt.Errorf("failed to generate hash id: %w", err)
//		}
//		cfg := fmt.Sprintf("random configuration for tenant %d", tenantID)
//		mutations[i] = spanner.Insert("feature_flags", []string{"tenant_id", "hash_id", "feature_flags_cfg"}, []interface{}{int64(tenantID), int64(hashID), cfg})
//	}
//
//	_, err = client.Apply(ctx, mutations)
//	if err != nil {
//		return fmt.Errorf("failed to insert testing data: %w", err)
//	}
//	return nil
//}

func url(project, instance, database string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)
}

func generateHashID(tenantID uint64) (uint64, error) {
	hash64 := fnv.New64a()
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, tenantID)
	_, err := hash64.Write(bytes)
	if err != nil {
		return 0, fmt.Errorf("failed to hash tenantID: %w", err)
	}

	// Mask the most significant bit to ensure non-negative int64 value
	hashValue := hash64.Sum64() &^ (1 << 63)
	return hashValue, nil
}
