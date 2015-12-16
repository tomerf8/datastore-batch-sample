package tasks

import (
  "net/http"

  "golang.org/x/net/context"
  "google.golang.org/appengine"
  "google.golang.org/appengine/datastore"
  "google.golang.org/appengine/delay"
  "google.golang.org/appengine/log"
)

const (
  QUERY_LIMIT = 100
  RATING_AVG  = 500
  ENTITY_NAME = "foo.bar"
)

var Data struct {

}

var (
  // query all player.unSyncstate
  q = datastore.NewQuery(ENTITY_NAME).Limit(QUERY_LIMIT)
)

func resetRating(ctx context.Context, cursorStr string, numUpdated int) {
  log.Debugf(ctx, "[Tasks.resetRating] initiated, cursorStr: %v", cursorStr)

  // set cursor if possible
  if cursorStr != "" {
    cursor, err := datastore.DecodeCursor(cursorStr)
    if err != nil {
      log.Errorf(ctx, "[Tasks.resetRating] error: %v", err)
      return
    } else {
      q = q.Start(cursor)
    }
  }

  // Iterate over the results.
  more := false
  t := q.Run(ctx)
  for {
    var s Foo
    key, err := t.Next(&s)
    if err == datastore.Done {
      break
    }
    if err != nil {
      log.Errorf(ctx, "[Tasks.resetRating] error fetching next state: %v", err)
      break
    }
    more = true
    // save for put
    keysToPut = append(keysToPut, key)
    statesToPut = append(statesToPut, s)
  }
  if len(keysToPut) > 0 {
    _, err := datastore.PutMulti(ctx, keysToPut, statesToPut)
    if err != nil {
      log.Errorf(ctx, "[Tasks.resetRating] error multiPut: %v", err)
    }
    numUpdated += len(keysToPut)
    log.Infof(ctx, "[Tasks.resetRating] Put %v entities to Datastore for a total of %v",
      len(keysToPut), numUpdated)
  }

  // Get updated cursor and store it for next time.
  if cursor, err := t.Cursor(); more && err == nil {
    resetRatingTask := delay.Func("resetRatingTaskKey", resetRating)
    resetRatingTask.Call(ctx, cursor.String(), numUpdated)
  }
  if !more {
    log.Infof(ctx, "[Tasks.resetRating] task complete with %v updates",
      numUpdated)
  }
}
