package main

import (
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/ipld/frisbii"
	car "github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipni/go-libipni/maurl"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

func loadCar(multicar *frisbii.MultiReadableStorage, carPath string) error {
	start := time.Now()
	logger.Infof("Opening CAR file [%s]...", carPath)
	carFile, err := os.Open(carPath)
	if err != nil {
		return err
	}
	store, err := carstorage.OpenReadable(carFile, car.UseWholeCIDs(false))
	if err != nil {
		return err
	}
	logger.Infof("CAR file [%s] opened in %s", carPath, time.Since(start))
	multicar.AddStore(store, store.Roots())
	return nil
}

func getListenAddr(serverAddr string, publicAddr string) (multiaddr.Multiaddr, error) {
	frisbiiAddr := "http://" + serverAddr
	if publicAddr != "" {
		frisbiiAddr = publicAddr
	}

	var frisbiiListenMaddr multiaddr.Multiaddr
	frisbiiUrl, err := url.Parse(frisbiiAddr)
	if err != nil {
		// try as multiaddr
		frisbiiListenMaddr, err = multiaddr.NewMultiaddr(frisbiiAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse public-addr [%s] as URL or multiaddr", frisbiiAddr)
		}
	} else {
		frisbiiListenMaddr, err = maurl.FromURL(frisbiiUrl)
		if err != nil {
			return nil, err
		}
	}

	return frisbiiListenMaddr, nil
}

func loadPrivKey(confDir string) (crypto.PrivKey, peer.ID, error) {
	// make the config dir in the user's home dir if it doesn't exist
	keyFile := path.Join(confDir, "key")
	data, err := os.ReadFile(keyFile)
	var privKey crypto.PrivKey
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, peer.ID(""), err
		}
		var err error
		privKey, _, err = crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, peer.ID(""), err
		}
		data, err := crypto.MarshalPrivateKey(privKey)
		if err != nil {
			return nil, peer.ID(""), err
		}
		if err := os.WriteFile(keyFile, data, 0600); err != nil {
			return nil, peer.ID(""), err
		}
	} else {
		var err error
		privKey, err = crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, peer.ID(""), err
		}
	}
	id, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return nil, peer.ID(""), err
	}
	return privKey, id, nil
}

func configDir() (string, error) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	confDir := path.Join(homedir, ConfigDir)
	if _, err := os.Stat(confDir); os.IsNotExist(err) {
		if err := os.Mkdir(confDir, 0700); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}
	return confDir, nil
}
