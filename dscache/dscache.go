package dscache

import (
	"fmt"
	"io/ioutil"

	golog "github.com/ipfs/go-log"
	"github.com/qri-io/dataset"
	dscachefb "github.com/qri-io/qri/dscache/dscachefb"
	"github.com/qri-io/qri/repo"
	"github.com/qri-io/qri/repo/profile"
)

var (
	log = golog.Logger("dscache")
)

// Dscache represents an in-memory serialized dscache flatbuffer
type Dscache struct {
	Root                *dscachefb.Dscache
	Buffer              []byte
	ProfileIDToUsername map[string]string
}

// LoadDscacheFromFile will load a dscache from the given filename
func LoadDscacheFromFile(filename string) (*Dscache, error) {
	buffer, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	root := dscachefb.GetRootAsDscache(buffer, 0)
	return &Dscache{Root: root, Buffer: buffer}, nil
}

// SaveTo writes the serialized bytes to the given filename
func (d *Dscache) SaveTo(filename string) error {
	return ioutil.WriteFile(filename, d.Buffer, 0644)
}

// Dump is a convenience function that dumps to the console, for debugging
func (d *Dscache) Dump() {
	fmt.Printf("Dscache:\n")
	for i := 0; i < d.Root.UsersLength(); i++ {
		userAssoc := dscachefb.UserAssoc{}
		d.Root.Users(&userAssoc, i)
		username := userAssoc.Username()
		profileID := userAssoc.ProfileID()
		fmt.Printf("%d) user=%s profileID=%s\n", i, username, profileID)
	}
	for i := 0; i < d.Root.RefsLength(); i++ {
		refCache := dscachefb.RefCache{}
		d.Root.Refs(&refCache, i)
		initID := refCache.InitID()
		prettyName := refCache.PrettyName()
		fsiPath := refCache.FsiPath()
		headRef := refCache.HeadRef()
		fmt.Printf("%d) initid=%s name=%s fsi=%s head=%s\n", i, initID, prettyName, fsiPath, headRef)
	}
}

// ListRefs returns references to each dataset in the cache
// TODO(dlong): Not alphabetized, which lib assumes it is
func (d *Dscache) ListRefs() ([]repo.DatasetRef, error) {
	d.ensureProToUserMap()
	refs := make([]repo.DatasetRef, 0, d.Root.RefsLength())
	for i := 0; i < d.Root.RefsLength(); i++ {
		refCache := dscachefb.RefCache{}
		d.Root.Refs(&refCache, i)
		profileID, err := profile.NewB58ID(string(refCache.ProfileID()))
		if err != nil {
			log.Errorf("could not parse profileID %q", string(refCache.ProfileID()))
			continue
		}

		refs = append(refs, repo.DatasetRef{
			Peername:  d.ProfileIDToUsername[string(refCache.ProfileID())],
			ProfileID: profileID,
			Name:      string(refCache.PrettyName()),
			Path:      string(refCache.HeadRef()),
			FSIPath:   string(refCache.FsiPath()),
			Dataset: &dataset.Dataset{
				Meta: &dataset.Meta{
					Title: string(refCache.MetaTitle()),
				},
				Structure: &dataset.Structure{
					ErrCount: int(refCache.NumErrors()),
					Entries:  int(refCache.BodyRows()),
					Length:   int(refCache.BodySize()),
				},
				Commit:      &dataset.Commit{},
				NumVersions: int(refCache.TopIndex()),
			},
		})
	}
	return refs, nil
}

func (d *Dscache) ensureProToUserMap() {
	if d.ProfileIDToUsername != nil {
		return
	}
	d.ProfileIDToUsername = make(map[string]string)
	for i := 0; i < d.Root.UsersLength(); i++ {
		userAssoc := dscachefb.UserAssoc{}
		d.Root.Users(&userAssoc, i)
		username := userAssoc.Username()
		profileID := userAssoc.ProfileID()
		d.ProfileIDToUsername[string(profileID)] = string(username)
	}
}