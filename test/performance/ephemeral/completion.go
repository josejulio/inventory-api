package ephemeral

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Each worker listens before writing, on its own connection. pgx retains
// notifications arriving before WaitForNotification, including those received
// while the persisted transaction ID is being queried. Unrelated notifications
// are discarded, with a bounded scan rather than an unbounded correlation map.
type completionListener struct{ conn *pgx.Conn }

func (f *fixtures) listen(ctx context.Context) (*completionListener, error) {
	conn, err := pgx.ConnectConfig(ctx, f.pool.Config().ConnConfig.Copy())
	if err != nil {
		return nil, fmt.Errorf("consumer completion connection failed")
	}
	if _, err = conn.Exec(ctx, `LISTEN consumer_notifications`); err != nil {
		conn.Close(ctx)
		return nil, err
	}
	return &completionListener{conn: conn}, nil
}
func (l *completionListener) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l.conn.Close(ctx)
}
func (f *fixtures) transactionID(ctx context.Context, instance, id string) (string, error) {
	var txid string
	err := f.pool.QueryRow(ctx, `SELECT rp.transaction_id FROM reporter_resources rr JOIN reporter_representations rp ON rp.reporter_resource_id=rr.id AND rp.version=rr.representation_version AND rp.generation=rr.generation WHERE rr.local_resource_id=$1 AND rr.reporter_instance_id=$2 AND rr.reporter_type='hbi' AND rr.resource_type='host'`, id, instance).Scan(&txid)
	if err != nil {
		return "", err
	}
	if txid == "" {
		return "", fmt.Errorf("report has no persisted transaction ID")
	}
	return txid, nil
}
func (l *completionListener) wait(ctx context.Context, txid string) error {
	if txid == "" {
		return fmt.Errorf("completion requires a transaction ID")
	}
	for discarded := 0; discarded < 10000; discarded++ {
		n, err := l.conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("consumer completion: %w", err)
		}
		if n.Channel == "consumer_notifications" && n.Payload == txid {
			return nil
		}
	}
	return fmt.Errorf("consumer completion exceeded 10000 unrelated notifications")
}
