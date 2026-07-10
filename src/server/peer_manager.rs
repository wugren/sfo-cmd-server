use crate::peer_connection::PeerConnection;
use crate::peer_id::PeerId;
use crate::server::CmdServerEventListener;
use crate::tunnel_id::{TunnelId, TunnelIdGenerator};
use crate::{CmdTunnelMeta, CmdTunnelRead, CmdTunnelWrite};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct CachedPeerInfo {
    conn_list: Vec<TunnelId>,
}

pub struct PeerManager<M: CmdTunnelMeta, R: CmdTunnelRead<M>, W: CmdTunnelWrite<M>> {
    conn_cache: Mutex<HashMap<TunnelId, (PeerId, Arc<PeerConnection<R, W>>)>>,
    device_conn_map: Mutex<HashMap<PeerId, CachedPeerInfo>>,
    conn_id_generator: TunnelIdGenerator,
    listener: Arc<dyn CmdServerEventListener>,
    _p: std::marker::PhantomData<M>,
}
pub type PeerManagerRef<M, R, W> = Arc<PeerManager<M, R, W>>;

impl<M: CmdTunnelMeta, R: CmdTunnelRead<M>, W: CmdTunnelWrite<M>> PeerManager<M, R, W> {
    pub fn new(listener: Arc<dyn CmdServerEventListener>) -> PeerManagerRef<M, R, W> {
        Arc::new(PeerManager {
            conn_cache: Mutex::new(HashMap::new()),
            device_conn_map: Mutex::new(HashMap::new()),
            conn_id_generator: TunnelIdGenerator::new(),
            listener,
            _p: Default::default(),
        })
    }

    pub fn generate_conn_id(&self) -> TunnelId {
        self.conn_id_generator.generate()
    }

    pub async fn add_peer_connection(self: &Arc<Self>, mut conn: PeerConnection<R, W>) {
        let recv_handle = conn.handle.take().unwrap();
        let peer_id = conn.peer_id.clone();
        let conn_id = conn.conn_id;
        let conn_count = {
            self.conn_cache
                .lock()
                .unwrap()
                .insert(conn_id, (peer_id.clone(), Arc::new(conn)));
            let mut device_conn_map = self.device_conn_map.lock().unwrap();
            let peer_info =
                device_conn_map
                    .entry(peer_id.clone())
                    .or_insert_with(|| CachedPeerInfo {
                        conn_list: Vec::new(),
                    });
            peer_info.conn_list.push(conn_id);
            peer_info.conn_list.len()
        };

        let this = self.clone();
        tokio::spawn(async move {
            let _ = recv_handle.await;
            this.remove_peer_connection(conn_id).await;
        });
        if conn_count == 1 {
            let _ = self.listener.on_peer_connected(&peer_id).await;
        }
    }

    pub async fn remove_peer_connection(&self, conn_id: TunnelId) {
        let disconnected_peer = {
            let mut conn_cache = self.conn_cache.lock().unwrap();
            let mut disconnected_peer = None;
            if let Some((peer_id, _)) = conn_cache.remove(&conn_id) {
                let mut device_conn_map = self.device_conn_map.lock().unwrap();
                if let Some(peer_info) = device_conn_map.get_mut(&peer_id) {
                    peer_info.conn_list.retain(|id| *id != conn_id);
                    if peer_info.conn_list.is_empty() {
                        device_conn_map.remove(&peer_id);
                        disconnected_peer = Some(peer_id);
                    }
                }
            }
            disconnected_peer
        };
        if let Some(peer_id) = disconnected_peer {
            let _ = self.listener.on_peer_disconnected(&peer_id).await;
        }
    }

    pub fn find_connection(
        &self,
        peer_id: &PeerId,
        conn_id: TunnelId,
    ) -> Option<Arc<PeerConnection<R, W>>> {
        let conn_cache = self.conn_cache.lock().unwrap();
        conn_cache
            .get(&conn_id)
            .filter(|(stored_peer_id, _)| stored_peer_id == peer_id)
            .map(|(_, conn)| conn.clone())
    }

    pub fn find_connections(&self, peer_id: &PeerId) -> Vec<Arc<PeerConnection<R, W>>> {
        let conn_cache = self.conn_cache.lock().unwrap();
        let device_conn_map = self.device_conn_map.lock().unwrap();
        device_conn_map
            .get(peer_id)
            .map(|peer_info| {
                peer_info
                    .conn_list
                    .iter()
                    .filter_map(|conn_id| conn_cache.get(conn_id).map(|(_, conn)| conn.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
}
