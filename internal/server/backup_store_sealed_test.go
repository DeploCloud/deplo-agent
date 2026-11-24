package server

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// sealedSend is a relayed backup as Deplo sends it to a destination that seals: header, data, then the trailer.
func sealedSend(target *pb.StoreTarget, data []byte, trailer *pb.StoreChunk_Trailer) []*pb.StoreChunk {
	msgs := framedStoreChunks(target, data)
	msgs[0].GetHeader().Sealed = true
	if trailer != nil {
		msgs = append(msgs, &pb.StoreChunk{Frame: &pb.StoreChunk_Trailer_{Trailer: trailer}})
	}
	return msgs
}

func trailerOf(b []byte) *pb.StoreChunk_Trailer {
	sum := sha256.Sum256(b)
	return &pb.StoreChunk_Trailer{SizeBytes: int64(len(b)), Sha256: hex.EncodeToString(sum[:])}
}

func TestWriteStoreFile_aSealedBackupIsKeptOnlyWholeAndChecked(t *testing.T) {
	whole := []byte("age-encryption.org/v1 and the rest of a backup")
	header := whole[:22]
	cases := []struct {
		name string
		msgs func(*pb.StoreTarget) []*pb.StoreChunk
		kept bool
	}{
		{"whole, with its trailer", func(st *pb.StoreTarget) []*pb.StoreChunk { return sealedSend(st, whole, trailerOf(whole)) }, true},
		// eu, 24 Nov 2026: a relay cut after the age header ended its stream with a clean close; 184 bytes were kept.
		{"cut after its header, closed cleanly", func(st *pb.StoreTarget) []*pb.StoreChunk { return sealedSend(st, header, nil) }, false},
		{"cut, with the trailer of the whole", func(st *pb.StoreTarget) []*pb.StoreChunk { return sealedSend(st, header, trailerOf(whole)) }, false},
		{"another backup's digest", func(st *pb.StoreTarget) []*pb.StoreChunk {
			tr := trailerOf(whole)
			tr.Sha256 = hex.EncodeToString(make([]byte, 32))
			return sealedSend(st, whole, tr)
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, root := newStoreService(t)
			target := &pb.StoreTarget{Root: root, ObjectKey: "prj_1/run.tar.gz.age"}
			ws := &fakeWriteStoreStream{msgs: c.msgs(target)}
			if err := s.WriteStoreFile(ws); err != nil {
				t.Fatal(err)
			}
			_, statErr := os.Stat(root + "/prj_1/run.tar.gz.age")
			if c.kept != ws.result.GetOk() || c.kept != (statErr == nil) {
				t.Fatalf("ok=%v kept on disk=%v, want %v (%s)", ws.result.GetOk(), statErr == nil, c.kept, ws.result.GetError())
			}
			if _, err := os.Stat(root + "/prj_1/run.tar.gz.age" + storePartialSuffix); err == nil {
				t.Fatal("the partial file stayed")
			}
		})
	}
}
