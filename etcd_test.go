package main

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
	"os"
	"testing"
)

func TestEtcdWal(t *testing.T) {
	dataPath := "./data"

	// 前回のテストで生成されたファイルを削除
	_ = os.RemoveAll(dataPath)

	logger := zap.NewExample()  // ロガーとしてzapを渡す必要がある
	metadata := []byte{1, 2, 3} // WALファイルに任意のMetadataを設定できる ReadAllの時に読み出せる

	// WALをファイルを作成してすぐに閉じる
	w, err := wal.Create(logger, dataPath, metadata)
	require.Nil(t, err)
	require.Nil(t, w.Close())

	// WALファイルを開く
	w, err = wal.Open(logger, dataPath, walpb.Snapshot{})
	require.Nil(t, err)

	{
		// WALファイルの中身を見てみる　まだ何も記録していないので空
		meta, state, ent, err := w.ReadAll()
		require.Nil(t, err)
		assert.Equal(t, []byte{1, 2, 3}, meta)
		assert.Equal(t, raftpb.HardState{
			Term:   0,
			Vote:   0,
			Commit: 0,
		}, state)
		assert.Len(t, ent, 0)
		t.Logf("meta %v state %v ent %v", meta, state, ent)
	}
	{
		// ファイルを開いてReadAllは1度しか呼び出すことが出来ない
		_, _, _, err := w.ReadAll()
		require.Equal(t, wal.ErrDecoderNotFound, err)
	}
	{
		// ログを追記する Saveの前にReadAllの呼び出しを行っておく必要がある
		// Raftの同期ステータスを上書き保存 内容は特にバリデーションされていない様子
		st := raftpb.HardState{
			Term:   0,
			Vote:   300,
			Commit: 200,
		}
		// Entryを追記 常にIndexを単調増加させる必要がある
		ents := []raftpb.Entry{
			{
				Term:  0,
				Index: 1,
				Data:  []byte{1},
			},
			{
				Term:  0,
				Index: 2,
				Data:  []byte{2},
			},
		}
		err := w.Save(st, ents)
		require.Nil(t, err)
	}
	{
		// .Saveを呼ぶ度にEntryが追記される
		st := raftpb.HardState{
			Term:   0,
			Vote:   300,
			Commit: 200,
		}
		ents := []raftpb.Entry{
			{
				Term:  0,
				Index: 3,
				Data:  []byte{3},
			},
		}
		err := w.Save(st, ents)
		require.Nil(t, err)
	}
	require.Nil(t, w.Close())

	// ReadAllを呼ぶために開き直す
	w, err = wal.Open(logger, dataPath, walpb.Snapshot{})
	require.Nil(t, err)
	{
		meta, state, ent, err := w.ReadAll()
		require.Nil(t, err)
		assert.Equal(t, []byte{1, 2, 3}, meta)
		assert.Equal(t, raftpb.HardState{
			Term:   0,
			Vote:   300,
			Commit: 200,
		}, state)
		assert.Equal(t, ent, []raftpb.Entry{
			{
				Term:  0,
				Index: 1,
				Data:  []byte{1},
			},
			{
				Term:  0,
				Index: 2,
				Data:  []byte{2},
			},
			{
				Term:  0,
				Index: 3,
				Data:  []byte{3},
			},
		})
		t.Logf("meta %v state %v ent %v", meta, state, ent)
	}
	require.Nil(t, w.Close())

	// ReadAllの内容はファイルを開き直しても変化しない
	// 1度のOpenで複数回呼べないけどMQみたいにConsumeで消えるわけでは無さそう
	w, err = wal.Open(logger, dataPath, walpb.Snapshot{})
	require.Nil(t, err)
	{
		meta, state, ent, err := w.ReadAll()
		require.Nil(t, err)
		assert.Equal(t, raftpb.HardState{
			Term:   0,
			Vote:   300,
			Commit: 200,
		}, state)
		assert.Len(t, ent, 3)
		t.Logf("meta %v state %v ent %v", meta, state, ent)
	}
	require.Nil(t, w.Close())
}
