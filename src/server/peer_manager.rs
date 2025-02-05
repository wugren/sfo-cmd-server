use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::peer_connection::PeerConnection;
use crate::peer_id::PeerId;
use crate::tunnel_id::{TunnelId, TunnelIdGenerator};

#[derive(Clone)]
pub struct CachedPeerInfo {
    pub conn_list: Vec<TunnelId>,
}

pub struct PeerManager {
    conn_cache: Mutex<HashMap<TunnelId, (PeerId, Arc<tokio::sync::Mutex<PeerConnection>>)>>,
    device_conn_map: Mutex<HashMap<PeerId, CachedPeerInfo>>,
    conn_id_generator: TunnelIdGenerator,
}
pub type PeerManagerRef = Arc<PeerManager>;


impl PeerManager {
    pub fn new() -> PeerManagerRef {
        Arc::new(PeerManager {
            conn_cache: Mutex::new(HashMap::new()),
            device_conn_map: Mutex::new(HashMap::new()),
            conn_id_generator: TunnelIdGenerator::new(),
        })
    }

    pub fn generate_conn_id(&self) -> TunnelId {
        self.conn_id_generator.generate()
    }

    pub fn add_peer_connection(self: &Arc<Self>, mut conn: PeerConnection) {
        let recv_handle = conn.handle.take().unwrap();
        let peer_id = conn.peer_id.clone();
        let conn_id = conn.conn_id;
        self.conn_cache.lock().unwrap().insert(conn_id, (peer_id.clone(), Arc::new(tokio::sync::Mutex::new(conn))));
        let mut device_conn_map = self.device_conn_map.lock().unwrap();
        let peer_info = device_conn_map.entry(peer_id).or_insert(CachedPeerInfo { conn_list: Vec::new() });
        peer_info.conn_list.push(conn_id);

        let this = self.clone();
        tokio::spawn(async move {
            let _ = recv_handle.await;
            this.remove_peer_connection(conn_id);
        });
    }

    pub fn remove_peer_connection(&self, conn_id: TunnelId) {
        let mut conn_cache = self.conn_cache.lock().unwrap();
        if let Some(conn) = conn_cache.remove(&conn_id) {
            let mut device_conn_map = self.device_conn_map.lock().unwrap();
            if let Some(peer_info) = device_conn_map.get_mut(&conn.0) {
                peer_info.conn_list.retain(|&id| id != conn_id);
                if peer_info.conn_list.is_empty() {
                    device_conn_map.remove(&conn.0);
                }
            }
        }
    }

    pub fn find_connection(&self, conn_id: TunnelId) -> Option<Arc<tokio::sync::Mutex<PeerConnection>>> {
        let conn_cache = self.conn_cache.lock().unwrap();
        conn_cache.get(&conn_id).map(|c| c.1.clone())
    }

    pub fn find_connections(&self, device_id: &PeerId) -> Vec<Arc<tokio::sync::Mutex<PeerConnection>>> {
        let conn_cache = self.conn_cache.lock().unwrap();
        let device_conn_map = self.device_conn_map.lock().unwrap();
        device_conn_map.get(device_id).map(|conns| {
            conns.conn_list.iter().filter_map(|c| conn_cache.get(c).map(|c| c.1.clone())).collect()
        }).unwrap_or_default()
    }

}
