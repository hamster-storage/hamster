package meta

import "fmt"

// Apply dispatches one decoded proposal to its Apply* method — the single
// entry point a Raft node applies committed log entries through. The result
// is whatever the specific apply returns (PutResult, DeleteObjectResult,
// …), nil for applies that return only an error. Deterministic like every
// apply: same proposal, same state, same outcome on every replica.
func (s *Store) Apply(p any) (any, error) {
	switch c := p.(type) {
	case CreateBucket:
		return nil, s.ApplyCreateBucket(c)
	case DeleteBucket:
		return nil, s.ApplyDeleteBucket(c)
	case SetBucketVersioning:
		return nil, s.ApplySetBucketVersioning(c)
	case SetObjectLockConfiguration:
		return nil, s.ApplySetObjectLockConfiguration(c)
	case PutObject:
		return s.ApplyPutObject(c)
	case DeleteObject:
		return s.ApplyDeleteObject(c)
	case DeleteVersion:
		return s.ApplyDeleteVersion(c)
	case UpdateRetention:
		return nil, s.ApplyUpdateRetention(c)
	case UpdateLegalHold:
		return nil, s.ApplyUpdateLegalHold(c)
	case CreateMultipartUpload:
		return nil, s.ApplyCreateMultipartUpload(c)
	case UploadPart:
		return s.ApplyUploadPart(c)
	case CompleteMultipartUpload:
		return s.ApplyCompleteMultipartUpload(c)
	case AbortMultipartUpload:
		return s.ApplyAbortMultipartUpload(c)
	case SetClusterLayout:
		return nil, s.ApplySetClusterLayout(c)
	case SetEncryptionPosture:
		return nil, s.ApplySetEncryptionPosture(c)
	case BeginKEKRotation:
		return nil, s.ApplyBeginKEKRotation(c)
	case RewrapDEK:
		return nil, s.ApplyRewrapDEK(c)
	case CompleteKEKRotation:
		return nil, s.ApplyCompleteKEKRotation(c)
	case RegisterNode:
		return nil, s.ApplyRegisterNode(c)
	case SetNodeDraining:
		return nil, s.ApplySetNodeDraining(c)
	case SetNodeReplacedBy:
		return nil, s.ApplySetNodeReplacedBy(c)
	case ReEncodeObject:
		return nil, s.ApplyReEncodeObject(c)
	default:
		return nil, fmt.Errorf("meta: Apply on unknown proposal type %T", p)
	}
}
